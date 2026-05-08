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
import logging
from typing import Any

import httpx

from config import settings

logger = logging.getLogger("bagent.services.wintact")

WINTACT_BASE_URL = "https://wintact.io/api"
WINTACT_WORKSPACE_ID = "booksyaudit"


class WintactError(RuntimeError):
    """Raised when wintact API call fails after retries."""


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
                f"Wintact {method} {path} HTTP {r.status_code}: {r.text[:300]}"
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
            payload["attributes"] = attributes
        return await self._post("/contacts.upsert", payload)

    async def batch_import_contacts(
        self, contacts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Bulk import — efektywniejsze od pojedynczych upsert dla seed
        51k salonów. Wintact internal limit 1k contacts per call (verify
        empirycznie)."""
        return await self._post("/contacts.batchImport", {"contacts": contacts})

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
        return await self._post("/lists.create", {
            "name": name,
            "description": description,
            "is_public": is_public,
            "is_double_optin": is_double_optin,
        })

    async def list_lists(self) -> list[dict[str, Any]]:
        result = await self._get("/lists.list")
        return result.get("lists", []) or []

    # ---------------------------------------------------------------
    # Templates (approved drafts deployowane do wintact)
    # ---------------------------------------------------------------

    async def create_template(
        self,
        name: str,
        subject: str,
        body: str,
        *,
        preview_text: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": name,
            "subject": subject,
            "body": body,
        }
        if preview_text:
            payload["preview_text"] = preview_text
        return await self._post("/templates.create", payload)

    async def update_template(
        self,
        template_id: str,
        *,
        subject: str | None = None,
        body: str | None = None,
        preview_text: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"id": template_id}
        if subject is not None:
            payload["subject"] = subject
        if body is not None:
            payload["body"] = body
        if preview_text is not None:
            payload["preview_text"] = preview_text
        return await self._post("/templates.update", payload)

    async def list_templates(self) -> list[dict[str, Any]]:
        result = await self._get("/templates.list")
        return result.get("templates", []) or []

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
