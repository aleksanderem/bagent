"""Wintact.io (Notifuse fork) API client.

Wintact ownsuje:
  - Email rendering (Mailgun via wintact)
  - Delivery tracking (open/click webhook events)
  - Contact storage (z custom attributes)
  - Templates compiled w czasie wysyłki

Bagent ownsuje:
  - State machine (parallel multi-state per kontakt × funnel)
  - Sequence orchestration (which template, when, conditional logic)
  - Approval gates (NIC nie idzie do wintact bez approved status)
  - Attribution loop (UTM click → audit_purchased detection)

API base: https://wintact.io/api/
Workspace: booksyaudit (single tenant)
Auth:      Bearer JWT (long-lived api_key, exp ~2036)
Rate cap:  25 sends/min (Mailgun EU) — enforced w outreach_orchestrator
"""

from __future__ import annotations

import asyncio
import re
import logging
from typing import Any

import httpx

from config import settings

logger = logging.getLogger("bagent.services.wintact")

WINTACT_BASE_URL = "https://wintact.io/api"
WINTACT_WORKSPACE_ID = "booksyaudit"


# Kody, pod którymi Wintact (fork Notifuse) melduje "ten obiekt już u nas
# jest": 409 byłoby poprawne, ale /templates.create odbija 400, a
# /lists.create potrafi 500. Sam kod NIGDY nie wystarcza — treść musi to
# potwierdzić, inaczej zwykłe 400/500 uznalibyśmy za sukces.
CONFLICT_STATUS_CODES = frozenset({400, 409, 422, 500})

# Dopasowanie po fragmencie, bez wielkości liter — treść komunikatu po
# stronie Wintacta bywa przeredagowana między wersjami.
CONFLICT_MESSAGE_MARKERS = (
    "already exists",
    "already exist",
    "duplicate",
    "already taken",
    "already in use",
)


class WintactError(RuntimeError):
    """Raised when wintact API call fails after retries.

    ``status_code``/``body`` są wypełniane dla odpowiedzi HTTP, żeby
    caller mógł odróżnić konflikt (obiekt już istnieje) od realnej awarii
    bez parsowania sformatowanego stringa."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body

    @property
    def is_conflict(self) -> bool:
        return is_conflict_error(self)


def is_conflict_error(exc: BaseException) -> bool:
    """True gdy błąd znaczy "ten obiekt już u nas jest".

    Wymaga OBU sygnałów: ZNANEGO kodu HTTP z listy konfliktowej ORAZ
    fragmentu komunikatu z ``CONFLICT_MESSAGE_MARKERS``.

    Brak kodu (``status_code is None``) to BRAK DOWODU, nie konflikt.
    ``_request`` rzuca właśnie tak po trzech nieudanych podejściach do
    429/5xx i po błędach sieci — żądanie mogło nigdy nie dojść, a w
    tekście siedzi cudzy komunikat (np. "failed after 3 retries: HTTP
    502: duplicate request blocked"). Uznanie tego za konflikt zapisałoby
    "wdrożone" dla obiektu, którego w Wintakcie nie ma."""
    status = getattr(exc, "status_code", None)
    if status is None or status not in CONFLICT_STATUS_CODES:
        return False
    haystack = f"{exc} {getattr(exc, 'body', '')}".casefold()
    return any(marker in haystack for marker in CONFLICT_MESSAGE_MARKERS)


def short_error(exc: BaseException) -> str:
    """Jednolinijkowy, ucięty opis błędu — do logu i do pól, które czyta
    człowiek. Ciała odpowiedzi 4xx/5xx bywają wielolinijkowym JSON-em albo
    HTML-em; wklejone w całości zasypują notatkę operatora."""
    return " ".join(str(exc).split())[:200]


def sanitize_list_id(name: str) -> str:
    """Notifuse wymaga STRICT alphanumeric id dla list (underscore = 400
    'id must be alphanumeric'). Zweryfikowane curl smoke 2026-08-11."""
    return re.sub(r"[^a-zA-Z0-9]", "", name)[:32] or "list"


def extract_template_id(resp: dict[str, Any]) -> str | None:
    """/templates.create zwraca {"template": {...}} albo płaski obiekt."""
    tpl = resp.get("template") or resp
    if not isinstance(tpl, dict):
        return None
    return tpl.get("id") or tpl.get("template_id")


class WintactClient:
    """Async wrapper for Wintact (Notifuse) REST API.

    Each call automatically appends ``workspace_id=booksyaudit`` query
    param OR injects into JSON body for POST endpoints. Auth via Bearer
    JWT (loaded from settings.wintact_api_key).
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = WINTACT_BASE_URL,
        workspace_id: str = WINTACT_WORKSPACE_ID,
        timeout: float = 30.0,
    ):
        self.api_key = api_key or getattr(settings, "wintact_api_key", "")
        if not self.api_key:
            raise RuntimeError("WINTACT_API_KEY not configured w bagent .env")
        self.base_url = base_url.rstrip("/")
        self.workspace_id = workspace_id
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Authorization": f"Bearer {self.api_key}"},
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    async def __aenter__(self) -> "WintactClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._client.aclose()

    # ---------------------------------------------------------------
    # Internal HTTP helpers
    # ---------------------------------------------------------------

    async def _get(
        self, path: str, params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        params = {**(params or {}), "workspace_id": self.workspace_id}
        return await self._request("GET", path, params=params)

    async def _post(
        self, path: str, data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = {**(data or {}), "workspace_id": self.workspace_id}
        return await self._request("POST", path, json=data)

    async def _request(
        self, method: str, path: str, **kwargs: Any,
    ) -> dict[str, Any]:
        """3-retry httpx wrapper z exponential backoff dla 429/5xx."""
        url = f"{self.base_url}{path}"
        last_error: str | None = None
        for attempt in range(3):
            try:
                r = await self._client.request(method, url, **kwargs)
            except httpx.RequestError as e:
                last_error = f"{type(e).__name__}: {e}"
                await asyncio.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                return r.json() if r.content else {}
            if r.status_code in (429, 502, 503, 504):
                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                await asyncio.sleep(2 ** attempt)
                continue
            # 4xx (non-retry) — surface immediately
            raise WintactError(
                f"Wintact {method} {path} HTTP {r.status_code}: {r.text[:300]}",
                status_code=r.status_code,
                body=r.text[:300],
            )
        raise WintactError(
            f"Wintact {method} {path} failed after 3 retries: {last_error}"
        )

    # ---------------------------------------------------------------
    # Contacts
    # ---------------------------------------------------------------

    async def upsert_contact(
        self,
        email: str,
        *,
        first_name: str | None = None,
        last_name: str | None = None,
        external_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create or update single contact.

        ``attributes`` are custom fields stored on contact (np. salon_ref_id,
        primary_category, desc_gap_pct). Used dla template variable
        substitution + segmentation filters."""
        payload: dict[str, Any] = {"email": email}
        if first_name:
            payload["first_name"] = first_name
        if last_name:
            payload["last_name"] = last_name
        if external_id:
            payload["external_id"] = external_id
        if attributes:
            # Notifuse trzyma custom_* jako pola TOP-LEVEL kontaktu
            payload.update(attributes)
        return await self._post("/contacts.upsert", {"contact": payload})

    async def upsert_custom_event(
        self,
        email: str,
        event_name: str,
        *,
        external_id: str,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """customEvents.upsert — natywny trigger automatyzacji Notifuse.
        external_id = klucz idempotencji (u nas outreach_events.external_key)."""
        payload: dict[str, Any] = {
            "email": email,
            "event_name": event_name,
            "external_id": external_id,
        }
        if properties:
            payload["properties"] = properties
        return await self._post("/customEvents.upsert", payload)

    async def batch_import_contacts(
        self, contacts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Bulk import — efektywniejsze od pojedynczych upsert dla seed
        51k salonów. Wintact internal limit 1k contacts per call (verify
        empirycznie)."""
        return await self._post("/contacts.import", {"contacts": contacts})

    async def get_contact_by_email(self, email: str) -> dict[str, Any] | None:
        try:
            return await self._get("/contacts.getByEmail", {"email": email})
        except WintactError as e:
            if "not found" in str(e).lower() or "404" in str(e):
                return None
            raise

    async def count_contacts(self) -> int:
        result = await self._get("/contacts.count")
        return int(result.get("total_contacts", 0))

    # ---------------------------------------------------------------
    # Lists (segmenty deployowane do wintact)
    # ---------------------------------------------------------------

    async def create_list(
        self, name: str, *, description: str = "", is_public: bool = False,
        is_double_optin: bool = False,
    ) -> dict[str, Any]:
        """Notifuse wymaga `id` — i w odróżnieniu od templates STRICT
        alphanumeric (underscore = 400 'id must be alphanumeric').
        Zweryfikowane curl smoke 2026-08-11. Sanityzujemy z name."""
        list_id = sanitize_list_id(name)
        resp = await self._post("/lists.create", {
            "id": list_id,
            "name": name[:32],
            "is_public": is_public,
            "is_double_optin": is_double_optin,
        })
        return resp.get("list") or resp

    async def list_lists(self) -> list[dict[str, Any]]:
        result = await self._get("/lists.list")
        return result.get("lists", []) or []

    async def find_list(self, list_id: str) -> dict[str, Any] | None:
        """Czy lista o tym id już u nich jest? Notifuse nie ma /lists.get,
        więc pytamy /lists.list. Gdy sam odczyt padnie — zwracamy None
        (brak dowodu), NIE udajemy że listy nie ma ani że jest."""
        try:
            lists = await self.list_lists()
        except WintactError as exc:
            logger.debug(
                "wintact /lists.list niedostępne przy weryfikacji %s: %s",
                list_id, short_error(exc),
            )
            return None
        for item in lists:
            if str(item.get("id") or "") == list_id:
                return item
        return None

    async def upsert_list(
        self, name: str, *, description: str = "", is_public: bool = False,
        is_double_optin: bool = False,
    ) -> dict[str, Any]:
        """Create-or-recognise. Zwraca {"id", "mode", "raw"}, gdzie mode to
        ``created`` albo ``existing``.

        /lists.create odbija duplikat 500 "Failed to create list" — kod i
        treść nie odróżniają go od realnej awarii Wintacta. Dlatego po
        KAŻDYM błędzie sprawdzamy fakt: czy lista o tym id jest na
        /lists.list. Jest → wdrożone (idempotentny sukces). Nie ma i
        komunikat nie mówi o duplikacie → realny błąd leci dalej."""
        wanted_id = sanitize_list_id(name)
        try:
            raw = await self.create_list(
                name, description=description, is_public=is_public,
                is_double_optin=is_double_optin,
            )
            return {
                "id": raw.get("id") or raw.get("list_id") or wanted_id,
                "mode": "created",
                "raw": raw,
            }
        except WintactError as exc:
            existing = await self.find_list(wanted_id)
            if existing is not None:
                logger.warning(
                    "wintact list %s już istnieje (%s) — zapisuję istniejącą, nie tworzę drugi raz",
                    wanted_id, short_error(exc),
                )
                return {
                    "id": existing.get("id") or wanted_id,
                    "mode": "existing",
                    "raw": existing,
                }
            if is_conflict_error(exc):
                logger.warning(
                    "wintact list %s zgłoszona jako duplikat (%s) — traktuję jako wdrożoną",
                    wanted_id, short_error(exc),
                )
                return {"id": wanted_id, "mode": "existing", "raw": {}}
            raise

    # ---------------------------------------------------------------
    # Templates (approved drafts deployowane do wintact)
    # ---------------------------------------------------------------

    async def create_template(
        self,
        template_id: str,
        name: str,
        subject: str,
        html: str,
        *,
        preview_text: str | None = None,
        category: str = "marketing",
    ) -> dict[str, Any]:
        """Create per Notifuse contract: id (slug ≤32), channel+category,
        email{subject, compiled_preview, visual_editor_tree}. Wysyłka i tak
        idzie przez /transactional.send z gotowym HTML (orchestrator), więc
        pusty tree MJML jest OK — template w wintact to kopia referencyjna.
        Drzewo MUSI nieść treść: send-path kompiluje visual_editor_tree,
        nie compiled_preview (internal/service/automation_node_executor.go)."""
        return await self._post(
            "/templates.create",
            self._template_payload(
                template_id, name, subject, html,
                preview_text=preview_text, category=category,
            ),
        )

    @staticmethod
    def _template_payload(
        template_id: str,
        name: str,
        subject: str,
        html: str,
        *,
        preview_text: str | None = None,
        category: str = "marketing",
    ) -> dict[str, Any]:
        """Jedno źródło kształtu szablonu dla create i update — inaczej
        update wysyłałby inny kontrakt niż ten zweryfikowany curl smokiem
        i konflikt kończyłby się drugim błędem zamiast naprawą treści."""
        styles = "".join(re.findall(r"<style[^>]*>.*?</style>", html, re.S))
        m = re.search(r"<body[^>]*>(.*)</body>", html, re.S)
        inner = m.group(1) if m else html
        tree = {"id": "root", "type": "mjml", "attributes": {}, "children": [
            {"id": "body", "type": "mj-body",
             "attributes": {"width": "640px"}, "children": [
                 {"id": "raw1", "type": "mj-raw", "attributes": {},
                  "content": styles + inner, "children": []}]}]}
        email: dict[str, Any] = {
            "subject": subject,
            "compiled_preview": html,
            "visual_editor_tree": tree,
        }
        if preview_text:
            email["subject_preview"] = preview_text
        return {
            "id": template_id[:32],
            "name": name[:32],
            "channel": "email",
            "category": category,
            "email": email,
        }

    async def update_template(
        self,
        template_id: str,
        name: str,
        subject: str,
        html: str,
        *,
        preview_text: str | None = None,
        category: str = "marketing",
    ) -> dict[str, Any]:
        """Podmiana treści istniejącego szablonu — ten sam kontrakt co
        create (Notifuse traktuje /templates.update jak pełny zapis, nie
        patch pojedynczych pól)."""
        return await self._post(
            "/templates.update",
            self._template_payload(
                template_id, name, subject, html,
                preview_text=preview_text, category=category,
            ),
        )

    async def upsert_template(
        self,
        template_id: str,
        name: str,
        subject: str,
        html: str,
        *,
        preview_text: str | None = None,
        category: str = "marketing",
    ) -> dict[str, Any]:
        """Create-or-update. Zwraca {"id", "mode", "raw"}, gdzie mode to
        ``created`` | ``updated`` | ``existing``.

        Notifuse nie ma upsertu — /templates.create na istniejącym id
        odbija 400 "Template id already exists". To NIE jest awaria:
        szablon u nich jest, więc podnosimy treść przez /templates.update.

        Gdy i update odpadnie, NIE wolno zwrócić sukcesu na słowo: kontrakt
        /templates.update nie jest zweryfikowany na żywym Wintakcie, więc
        jego porażka nie mówi nic o tym, czy szablon tam faktycznie jest.
        Caller zapisuje po nas approval_status='deployed', a orchestrator
        wysyła do prospektów wszystko, co jest 'deployed' — sukces bez
        pokrycia oznacza wysyłkę szablonu ze starą albo nieistniejącą
        treścią. Dlatego pytamy o FAKT: /templates.list. Jest → ``existing``
        (stara treść, ale realny szablon). Nie ma albo lista niedostępna →
        wyjątek, wiersz zostaje w 'approved' i wróci w kolejnym cyklu."""
        wanted_id = template_id[:32]
        try:
            raw = await self.create_template(
                template_id, name, subject, html,
                preview_text=preview_text, category=category,
            )
            return {
                "id": extract_template_id(raw) or wanted_id,
                "mode": "created",
                "raw": raw,
            }
        except WintactError as exc:
            if not is_conflict_error(exc):
                raise
            conflict = exc

        try:
            raw = await self.update_template(
                template_id, name, subject, html,
                preview_text=preview_text, category=category,
            )
            return {
                "id": extract_template_id(raw) or wanted_id,
                "mode": "updated",
                "raw": raw,
            }
        except WintactError as upd_exc:
            existing = await self.find_template(wanted_id)
            if existing is None:
                raise WintactError(
                    f"Wintact template {wanted_id}: /templates.create zgłosiło konflikt, "
                    f"/templates.update padło ({short_error(upd_exc)}), a /templates.list "
                    f"nie potwierdza, że szablon tam jest — NIE oznaczam jako wdrożony",
                    status_code=upd_exc.status_code,
                    body=upd_exc.body,
                ) from upd_exc
            logger.warning(
                "wintact template %s jest w wintakcie (potwierdzone /templates.list po "
                "konflikcie: %s), ale /templates.update padło (%s) — zapisuję jako "
                "wdrożony ze STARĄ treścią",
                wanted_id, short_error(conflict), short_error(upd_exc),
            )
            return {
                "id": existing.get("id") or wanted_id,
                "mode": "existing",
                "raw": existing,
            }

    async def list_templates(self) -> list[dict[str, Any]]:
        result = await self._get("/templates.list")
        return result.get("templates", []) or []

    async def find_template(self, template_id: str) -> dict[str, Any] | None:
        """Czy szablon o tym id jest u nich NAPRAWDĘ? Notifuse nie ma
        /templates.get, więc pytamy /templates.list — to jedyny dowód,
        jakim dysponujemy. Gdy sam odczyt padnie, zwracamy None (brak
        dowodu), NIE udajemy że szablon tam jest."""
        try:
            templates = await self.list_templates()
        except WintactError as exc:
            logger.warning(
                "wintact /templates.list niedostępne przy weryfikacji %s: %s",
                template_id, short_error(exc),
            )
            return None
        for item in templates:
            if str(item.get("id") or "") == template_id:
                return item
        return None

    async def compile_template(
        self,
        template_id: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        """Render template z variables — dry-run dla preview UI."""
        return await self._post("/templates.compile", {
            "id": template_id,
            "variables": variables,
        })

    # ---------------------------------------------------------------
    # Transactional send (główny outreach send path)
    # ---------------------------------------------------------------

    async def send_transactional(
        self,
        contact_email: str,
        template_id: str,
        *,
        variables: dict[str, Any] | None = None,
        sender_email: str | None = None,
        sender_name: str | None = None,
        utm: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Render + send pojedynczy email do contact'u via wintact.

        Returns: {"message_id": "..."} — do logowania w outreach_messages
        dla webhook event correlation.
        """
        payload: dict[str, Any] = {
            "contact_email": contact_email,
            "template_id": template_id,
        }
        if variables:
            payload["variables"] = variables
        if sender_email:
            payload["sender_email"] = sender_email
        if sender_name:
            payload["sender_name"] = sender_name
        if utm:
            payload["utm"] = utm
        return await self._post("/transactional.send", payload)

    # ---------------------------------------------------------------
    # Webhooks (subscription dla bagent webhook handler)
    # ---------------------------------------------------------------

    async def list_webhooks(self) -> list[dict[str, Any]]:
        result = await self._get("/webhooks.list")
        return result.get("webhooks", []) or []

    async def create_webhook(
        self,
        url: str,
        event_types: list[str],
        *,
        name: str = "bagent webhook",
    ) -> dict[str, Any]:
        return await self._post("/webhooks.create", {
            "url": url,
            "event_types": event_types,
            "name": name,
        })

    async def get_webhook_event_types(self) -> list[str]:
        result = await self._get("/webhooks.eventTypes")
        return result.get("event_types", []) or []

    # ---------------------------------------------------------------
    # Workspace introspection
    # ---------------------------------------------------------------

    async def workspace_info(self) -> dict[str, Any]:
        result = await self._get("/workspaces.list")
        # /workspaces.list returns array, find ours
        if isinstance(result, list):
            for ws in result:
                if ws.get("id") == self.workspace_id:
                    return ws
        return {}


# ---------------------------------------------------------------------
# Convenience helpers — used by orchestrator
# ---------------------------------------------------------------------

async def healthcheck() -> dict[str, Any]:
    """Quick connectivity verify dla deploy smoke test."""
    async with WintactClient() as wc:
        try:
            count = await wc.count_contacts()
            ws = await wc.workspace_info()
            return {
                "ok": True,
                "workspace_name": ws.get("name"),
                "contacts": count,
                "tracking_enabled": ws.get("settings", {}).get("email_tracking_enabled"),
            }
        except WintactError as e:
            return {"ok": False, "error": str(e)}
