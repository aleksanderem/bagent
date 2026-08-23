"""Deployer outreachu: wdrożone tylko wtedy, gdy obiekt NAPRAWDĘ tam jest.

Kontrakty pinowane tutaj (BEAUTY_AUDIT-tu2m / BEAUTY_AUDIT-mp8u):

  1. "already exists" z /templates.create NIE jest awarią — treść idzie
     przez /templates.update, a stan w Supabase zapisuje się dokładnie tak
     jak przy świeżym utworzeniu (bez tego cron wracał po ten sam wiersz
     co 2 minuty — 70 błędów/h);
  2. gdy /templates.update padnie, sukces wymaga DOWODU z /templates.list.
     Bez potwierdzenia leci wyjątek i wiersz NIE dostaje 'deployed' —
     inaczej orchestrator zacząłby wysyłać do prospektów szablon ze starą
     albo nieistniejącą treścią;
  3. brak kodu HTTP to brak dowodu duplikatu: WintactError po trzech
     nieudanych retry niesie cudzy tekst ("HTTP 502: duplicate request
     blocked") i NIE może uchodzić za konflikt;
  4. approval_notes to pole człowieka — nasz ślad jest zawsze jedną
     linią pod separatorem, a notatka operatora wraca dosłownie;
  5. awaria Wintacta NIE cofa zatwierdzenia człowieka: wiersz zostaje
     nietknięty w 'approved'. Licznik prób i degradacja statusu dotyczą
     wyłącznie trwałych błędów danych (4xx z walidacji).

Zero żywego HTTP i zero żywej bazy — klient wintacta i supabase wstrzyknięte.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.wintact import WintactClient, WintactError, is_conflict_error
from workers import outreach_deployer as dep


TEMPLATE_CONFLICT_BODY = '{"error":"Template id already exists"}'
LIST_500_BODY = '{"error":"Failed to create list"}'

# 400 z walidacji payloadu — jedyny rodzaj błędu, za który wiersz płaci
# licznikiem prób i w końcu wraca do kolejki zatwierdzeń.
PERMANENT_ERROR = dict(
    status_code=400, body='{"error":"email.subject must not be empty"}',
)

TEMPLATE_ROW = {
    "id": "424cb089-005b-470d-a6e9-9dd9bfda2e2c",
    "funnel": "audit",
    "step_key": "d0_cold",
    "vertical": "beauty_mass",
    "variant_label": "A",
    "subject": "Twój profil Booksy",
    "body_md": "",
    "body_html": "<html><body>Cześć</body></html>",
    "preview_text": None,
    "approval_notes": None,
}

SEGMENT_ROW = {
    "id": "2e5c6bca-88d5-4fe4-bd1c-c9e2948477ca",
    "funnel": "audit",
    "name": "warszawa",
    "description": None,
    "cohort": "scale",
    "approval_notes": None,
}


# ---------------------------------------------------------------------------
# Podwójniaki
# ---------------------------------------------------------------------------

class FakeTable:
    """Łańcuszek supabase-py: select/eq/is_/limit → execute zwraca wiersze,
    update(...).eq(...).execute() zapisuje patch do ``updates``."""

    def __init__(self, rows: list[dict]):
        self.rows = rows
        self.updates: list[dict] = []
        self._patch: dict | None = None

    def select(self, *a, **k):  # noqa: ANN002, ANN003
        return self

    def eq(self, *a, **k):  # noqa: ANN002, ANN003
        return self

    def is_(self, *a, **k):  # noqa: ANN002, ANN003
        return self

    def limit(self, *a, **k):  # noqa: ANN002, ANN003
        return self

    def update(self, patch):  # noqa: ANN001
        self._patch = dict(patch)
        return self

    def execute(self):
        if self._patch is not None:
            self.updates.append(self._patch)
            self._patch = None
            return SimpleNamespace(data=[])
        return SimpleNamespace(data=self.rows)


def make_sb(table_name: str, rows: list[dict]) -> tuple[MagicMock, FakeTable]:
    table = FakeTable(rows)

    def _table(name):  # noqa: ANN001
        if name != table_name:
            raise AssertionError(f"deployer dotknął nieoczekiwanej tabeli: {name}")
        return table

    sb = MagicMock()
    sb.table.side_effect = _table
    return sb, table


def make_wc(**methods) -> AsyncMock:  # noqa: ANN003
    wc = AsyncMock()
    wc.__aenter__ = AsyncMock(return_value=wc)
    wc.__aexit__ = AsyncMock(return_value=False)
    for name, impl in methods.items():
        setattr(wc, name, impl)
    return wc


def real_client_factory():
    """Podstawiane pod ``dep.WintactClient`` tam, gdzie chcemy przepuścić
    deployer przez PRAWDZIWĄ logikę upsertu, a nie przez AsyncMocka."""
    return lambda *a, **k: WintactClient(api_key="test")


def wire_template_http(monkeypatch, *, update_fails: bool, listed: list[dict]):
    """create zawsze odbija konfliktem; update opcjonalnie pada;
    /templates.list zwraca to, co podamy."""

    async def fake_post(self, path, data=None):  # noqa: ANN001
        if path == "/templates.create":
            raise WintactError(
                f"Wintact POST /templates.create HTTP 400: {TEMPLATE_CONFLICT_BODY}",
                status_code=400, body=TEMPLATE_CONFLICT_BODY,
            )
        if update_fails:
            raise WintactError(
                "Wintact POST /templates.update HTTP 400: unknown field",
                status_code=400, body="unknown field",
            )
        return {"template": {"id": data["id"]}}

    async def fake_get(self, path, params=None):  # noqa: ANN001
        assert path == "/templates.list"
        return {"templates": listed}

    monkeypatch.setattr(WintactClient, "_post", fake_post)
    monkeypatch.setattr(WintactClient, "_get", fake_get)


# ---------------------------------------------------------------------------
# Klasyfikacja: konflikt vs realny błąd
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "status, body, expected",
    [
        (400, TEMPLATE_CONFLICT_BODY, True),          # tu2m — produkcyjny komunikat
        (400, '{"error":"TEMPLATE ID ALREADY EXISTS"}', True),   # inna wielkość liter
        (400, '{"error":"template already exists in workspace"}', True),  # inna treść
        (409, '{"error":"duplicate list id"}', True),
        (500, LIST_500_BODY, False),                  # mp8u — realna awaria
        (401, '{"error":"token already exists"}', False),  # zły kod → nie konflikt
        (404, '{"error":"already exists"}', False),   # 404 nie jest konfliktem
    ],
)
def test_conflict_classification(status: int, body: str, expected: bool):
    exc = WintactError(f"Wintact POST /x HTTP {status}: {body}", status_code=status, body=body)
    assert is_conflict_error(exc) is expected
    assert exc.is_conflict is expected


@pytest.mark.parametrize("message", [
    # Dokładny dowód z bramki weryfikatora: po 3 retry NIE MA kodu HTTP,
    # a w tekście siedzi cudzy komunikat 502 ze słowem "duplicate".
    "Wintact POST /templates.create failed after 3 retries: "
    "HTTP 502: duplicate request blocked",
    # To samo, tylko z frazą produkcyjną z /templates.create.
    "Wintact POST /templates.create failed after 3 retries: "
    "HTTP 503: Template id already exists",
    "Wintact POST /lists.create failed after 3 retries: ConnectError: [Errno 61]",
])
def test_missing_status_code_is_never_a_conflict(message: str):
    """Brak kodu HTTP = brak dowodu, że obiekt powstał. Uznanie tego za
    konflikt zapisałoby 'deployed' dla szablonu, którego tam nie ma."""
    exc = WintactError(message)
    assert exc.status_code is None
    assert is_conflict_error(exc) is False
    assert exc.is_conflict is False


# ---------------------------------------------------------------------------
# Klient: upsert szablonu
# ---------------------------------------------------------------------------

async def test_upsert_template_conflict_refreshes_content_via_update(monkeypatch):
    calls: list[str] = []

    async def fake_post(self, path, data=None):  # noqa: ANN001
        calls.append(path)
        if path == "/templates.create":
            raise WintactError(
                f"Wintact POST /templates.create HTTP 400: {TEMPLATE_CONFLICT_BODY}",
                status_code=400,
                body=TEMPLATE_CONFLICT_BODY,
            )
        assert data["email"]["subject"] == "Temat"      # update dostaje kontrakt create'a
        assert data["id"] == "d0_cold"
        return {"template": {"id": "d0_cold"}}

    monkeypatch.setattr(WintactClient, "_post", fake_post)
    async with WintactClient(api_key="test") as wc:
        result = await wc.upsert_template(
            "d0_cold", "d0_cold", "Temat", "<html><body>x</body></html>",
        )

    assert calls == ["/templates.create", "/templates.update"]
    assert result == {"id": "d0_cold", "mode": "updated", "raw": {"template": {"id": "d0_cold"}}}


async def test_upsert_template_broken_update_needs_proof_from_list(monkeypatch, caplog):
    """Update padł, ale /templates.list POTWIERDZA, że szablon tam jest →
    'existing' (stara treść, lecz realny szablon)."""
    wire_template_http(
        monkeypatch, update_fails=True,
        listed=[{"id": "inny"}, {"id": "d0_cold", "name": "d0_cold"}],
    )
    with caplog.at_level(logging.WARNING, logger="bagent.services.wintact"):
        async with WintactClient(api_key="test") as wc:
            result = await wc.upsert_template(
                "d0_cold", "d0_cold", "Temat", "<html><body>x</body></html>",
            )

    assert result["mode"] == "existing" and result["id"] == "d0_cold"
    assert result["raw"] == {"id": "d0_cold", "name": "d0_cold"}
    assert [r.levelno for r in caplog.records] == [logging.WARNING]
    assert caplog.records[0].exc_info is None      # jedno zdanie, bez tracebacku


async def test_upsert_template_broken_update_without_proof_raises(monkeypatch):
    """Update padł i szablonu NIE MA na /templates.list → wyjątek.
    Nie wolno zwrócić sukcesu na słowo /templates.update — jego kontrakt
    nie jest zweryfikowany na żywym Wintakcie."""
    wire_template_http(monkeypatch, update_fails=True, listed=[{"id": "cos_innego"}])
    async with WintactClient(api_key="test") as wc:
        with pytest.raises(WintactError, match="NIE oznaczam jako wdrożony"):
            await wc.upsert_template("d0_cold", "d0_cold", "Temat", "<p>x</p>")


async def test_upsert_template_raises_when_list_itself_is_unavailable(monkeypatch):
    """Niedostępna lista to brak dowodu, a nie domniemanie istnienia."""
    async def fake_post(self, path, data=None):  # noqa: ANN001
        if path == "/templates.create":
            raise WintactError(
                f"Wintact POST /templates.create HTTP 400: {TEMPLATE_CONFLICT_BODY}",
                status_code=400, body=TEMPLATE_CONFLICT_BODY,
            )
        raise WintactError(
            "Wintact POST /templates.update HTTP 500: boom", status_code=500, body="boom",
        )

    async def fake_get(self, path, params=None):  # noqa: ANN001
        raise WintactError("Wintact GET /templates.list failed after 3 retries: timeout")

    monkeypatch.setattr(WintactClient, "_post", fake_post)
    monkeypatch.setattr(WintactClient, "_get", fake_get)
    async with WintactClient(api_key="test") as wc:
        with pytest.raises(WintactError, match="NIE oznaczam jako wdrożony"):
            await wc.upsert_template("d0_cold", "d0_cold", "Temat", "<p>x</p>")


async def test_upsert_template_real_error_propagates(monkeypatch):
    async def fake_post(self, path, data=None):  # noqa: ANN001
        raise WintactError(
            'Wintact POST /templates.create HTTP 401: {"error":"invalid token"}',
            status_code=401, body='{"error":"invalid token"}',
        )

    monkeypatch.setattr(WintactClient, "_post", fake_post)
    async with WintactClient(api_key="test") as wc:
        with pytest.raises(WintactError, match="401"):
            await wc.upsert_template("d0_cold", "d0_cold", "Temat", "<p>x</p>")


# ---------------------------------------------------------------------------
# Klient: upsert listy (500 rozstrzygane faktem z /lists.list)
# ---------------------------------------------------------------------------

def _list_client(monkeypatch, existing: list[dict]):
    async def fake_post(self, path, data=None):  # noqa: ANN001
        raise WintactError(
            f"Wintact POST /lists.create HTTP 500: {LIST_500_BODY}",
            status_code=500, body=LIST_500_BODY,
        )

    async def fake_get(self, path, params=None):  # noqa: ANN001
        assert path == "/lists.list"
        return {"lists": existing}

    monkeypatch.setattr(WintactClient, "_post", fake_post)
    monkeypatch.setattr(WintactClient, "_get", fake_get)


async def test_upsert_list_500_on_existing_list_is_idempotent_success(monkeypatch):
    _list_client(monkeypatch, [{"id": "auditscalewarszawa", "name": "audit__scale__warszawa"}])
    async with WintactClient(api_key="test") as wc:
        result = await wc.upsert_list("audit__scale__warszawa")

    assert result["mode"] == "existing"
    assert result["id"] == "auditscalewarszawa"


async def test_upsert_list_500_without_existing_list_propagates(monkeypatch):
    _list_client(monkeypatch, [])
    async with WintactClient(api_key="test") as wc:
        with pytest.raises(WintactError, match="500"):
            await wc.upsert_list("audit__scale__warszawa")


# ---------------------------------------------------------------------------
# Deployer × prawdziwy upsert: kiedy wiersz dostaje 'deployed'
# ---------------------------------------------------------------------------

async def test_unproven_template_never_reaches_deployed_status(monkeypatch):
    """(a) create=400 already exists + update pada + lista NIE potwierdza
    → żadnego 'deployed', żadnego wintact_template_id. Gdyby to przeszło,
    orchestrator zacząłby wysyłać ten szablon do prospektów."""
    wire_template_http(monkeypatch, update_fails=True, listed=[])
    sb, table = make_sb(dep.TEMPLATES_TABLE, [dict(TEMPLATE_ROW)])

    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", real_client_factory()):
        out = await dep.deploy_approved_templates({})

    assert out == {"deployed": 0, "reconciled": 0, "errors": 1}
    [written] = table.updates
    assert "wintact_template_id" not in written
    assert written.get("approval_status") != "deployed"


async def test_template_confirmed_by_list_is_recorded_as_deployed(monkeypatch):
    """(b) ta sama ścieżka, ale /templates.list POTWIERDZA istnienie →
    wiersz dostaje 'deployed' i przestaje wracać w każdym cyklu crona."""
    wire_template_http(
        monkeypatch, update_fails=True, listed=[{"id": "d0_cold", "name": "d0_cold"}],
    )
    sb, table = make_sb(dep.TEMPLATES_TABLE, [dict(TEMPLATE_ROW)])

    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", real_client_factory()):
        out = await dep.deploy_approved_templates({})

    assert out == {"deployed": 1, "reconciled": 1, "errors": 0}
    [written] = table.updates
    assert written["wintact_template_id"] == "d0_cold"
    assert written["approval_status"] == "deployed"


# ---------------------------------------------------------------------------
# Deployer: szablony
# ---------------------------------------------------------------------------

async def test_template_conflict_is_recorded_as_deployed(caplog):
    sb, table = make_sb(dep.TEMPLATES_TABLE, [dict(TEMPLATE_ROW)])
    wc = make_wc(upsert_template=AsyncMock(
        return_value={"id": "d0_cold", "mode": "updated", "raw": {}},
    ))

    with caplog.at_level(logging.DEBUG, logger="bagent.workers.outreach_deployer"), \
         patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=wc):
        out = await dep.deploy_approved_templates({})

    assert out == {"deployed": 1, "reconciled": 1, "errors": 0}
    [written] = table.updates
    assert written["wintact_template_id"] == "d0_cold"
    assert written["approval_status"] == "deployed"
    assert written["deployed_at"]
    # żaden ERROR ani traceback — to jest cała istota tu2m
    assert [r.levelno for r in caplog.records] == [logging.WARNING]
    assert caplog.records[0].exc_info is None


async def test_template_permanent_error_does_not_write_deployed_state(caplog):
    sb, table = make_sb(dep.TEMPLATES_TABLE, [dict(TEMPLATE_ROW)])
    wc = make_wc(upsert_template=AsyncMock(side_effect=WintactError(
        'Wintact POST /templates.create HTTP 400: {"error":"email.subject must not be empty"}',
        **PERMANENT_ERROR,
    )))

    with caplog.at_level(logging.DEBUG, logger="bagent.workers.outreach_deployer"), \
         patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=wc):
        out = await dep.deploy_approved_templates({})

    assert out == {"deployed": 0, "reconciled": 0, "errors": 1}
    [written] = table.updates
    assert "wintact_template_id" not in written
    assert "approval_status" not in written           # nadal 'approved', spróbuje jeszcze raz
    assert "[deploy-failed 1/3]" in written["approval_notes"]
    assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)


# ---------------------------------------------------------------------------
# Awaria usługi NIE cofa decyzji człowieka
# ---------------------------------------------------------------------------

OUTAGE_ERRORS = {
    # Tak wygląda 502/503/504 i błąd sieci PO stronie ``_request``: trzy
    # nieudane podejścia, wyjątek BEZ kodu HTTP. To jest realny kształt
    # awarii w produkcji.
    "retries_exhausted": WintactError(
        "Wintact POST /templates.create failed after 3 retries: HTTP 502: bad gateway",
    ),
    "http_500": WintactError(
        "Wintact POST /templates.create HTTP 500: internal error",
        status_code=500, body="internal error",
    ),
    "bad_api_key": WintactError(
        'Wintact POST /templates.create HTTP 401: {"error":"invalid token"}',
        status_code=401, body='{"error":"invalid token"}',
    ),
}


@pytest.mark.parametrize("kind", sorted(OUTAGE_ERRORS))
async def test_service_outage_never_downgrades_human_approval(kind: str, caplog):
    """(e) Cron co 2 min × awaria Wintacta = w 6 minut wszystkie
    zatwierdzone szablony wypadłyby z 'approved'. Wiersz ma zostać
    NIETKNIĘTY, choćby awaria trwała dziesięć cykli."""
    exc = OUTAGE_ERRORS[kind]
    human_note = "Zatwierdzam. Wysyłka dopiero po 15.09 — AM"

    for cycle in range(dep.MAX_DEPLOY_ATTEMPTS + 2):
        row = dict(TEMPLATE_ROW, approval_notes=human_note)
        sb, table = make_sb(dep.TEMPLATES_TABLE, [row])
        wc = make_wc(upsert_template=AsyncMock(side_effect=exc))

        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="bagent.workers.outreach_deployer"), \
             patch.object(dep, "make_supabase_client", return_value=sb), \
             patch.object(dep, "WintactClient", return_value=wc):
            out = await dep.deploy_approved_templates({})

        assert out == {"deployed": 0, "reconciled": 0, "errors": 1}
        assert table.updates == [], f"cykl {cycle}: awaria ruszyła wiersz w bazie"

    # Log jest, ale jednolinijkowy — bez tracebacku zalewającego dyżurnego.
    assert [r.levelno for r in caplog.records] == [logging.ERROR]
    assert caplog.records[0].exc_info is None


async def test_segment_service_outage_leaves_row_untouched():
    sb, table = make_sb(dep.SEGMENTS_TABLE, [dict(SEGMENT_ROW)])
    wc = make_wc(upsert_list=AsyncMock(side_effect=WintactError(
        f"Wintact POST /lists.create HTTP 500: {LIST_500_BODY}",
        status_code=500, body=LIST_500_BODY,
    )))

    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=wc):
        out = await dep.deploy_approved_segments({})

    assert out == {"deployed": 0, "reconciled": 0, "errors": 1}
    assert table.updates == []


def test_transient_classification():
    assert dep._is_transient_failure(WintactError("no code")) is True
    assert dep._is_transient_failure(
        WintactError("x", status_code=503, body="")) is True
    assert dep._is_transient_failure(
        WintactError("x", status_code=401, body="")) is True
    assert dep._is_transient_failure(
        WintactError("x", status_code=400, body="")) is False
    # Nie-WintactError to wysypka na naszych danych (renderer, brak pola).
    assert dep._is_transient_failure(KeyError("subject")) is False


# ---------------------------------------------------------------------------
# Deployer: segmenty
# ---------------------------------------------------------------------------

async def test_segment_conflict_is_recorded_as_deployed():
    sb, table = make_sb(dep.SEGMENTS_TABLE, [dict(SEGMENT_ROW)])
    wc = make_wc(upsert_list=AsyncMock(
        return_value={"id": "auditscalewarszawa", "mode": "existing", "raw": {}},
    ))

    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=wc):
        out = await dep.deploy_approved_segments({})

    assert out == {"deployed": 1, "reconciled": 1, "errors": 0}
    [written] = table.updates
    assert written["wintact_list_id"] == "auditscalewarszawa"
    assert written["approval_status"] == "deployed"


async def test_segment_permanent_error_counts_attempt_then_parks_for_review():
    failing = AsyncMock(side_effect=WintactError(
        'Wintact POST /lists.create HTTP 400: {"error":"id must be alphanumeric"}',
        status_code=400, body='{"error":"id must be alphanumeric"}',
    ))

    # 1. próba — licznik rośnie, wiersz zostaje w 'approved'
    first_row = dict(SEGMENT_ROW, approval_notes="ok, zatwierdzam — AM")
    sb, table = make_sb(dep.SEGMENTS_TABLE, [first_row])
    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=make_wc(upsert_list=failing)):
        out = await dep.deploy_approved_segments({})

    assert out == {"deployed": 0, "reconciled": 0, "errors": 1}
    [written] = table.updates
    assert "approval_status" not in written
    assert "[deploy-failed 1/3]" in written["approval_notes"]
    assert written["approval_notes"].startswith("ok, zatwierdzam — AM")  # notatka pierwsza
    assert "wintact_list_id" not in written

    # 3. próba — wiersz wypada z kolejki deployu, ląduje w kolejce zatwierdzeń
    third_row = dict(SEGMENT_ROW, approval_notes=written["approval_notes"].replace("1/3", "2/3"))
    sb2, table2 = make_sb(dep.SEGMENTS_TABLE, [third_row])
    with patch.object(dep, "make_supabase_client", return_value=sb2), \
         patch.object(dep, "WintactClient", return_value=make_wc(upsert_list=failing)):
        await dep.deploy_approved_segments({})

    [parked] = table2.updates
    assert parked["approval_status"] == "changes_requested"
    assert "[deploy-failed 3/3]" in parked["approval_notes"]
    assert parked["approval_notes"].startswith("ok, zatwierdzam — AM")


# ---------------------------------------------------------------------------
# approval_notes — pole człowieka, nie wysypisko
# ---------------------------------------------------------------------------

MULTILINE_BODY = (
    'Wintact POST /templates.create HTTP 400: {\n'
    '  "error": "validation failed",\n'
    '  "fields": [\n'
    '    "email.subject"\n'
    '  ]\n'
    '}'
)


async def test_multiline_error_lands_as_single_line_above_nothing(caplog):
    """(d) Ciało 400/500 bywa wielolinijkowym JSON-em. W polu operatora ma
    zostać JEDNA linia, a jego tekst ma wrócić dosłownie."""
    human = "Zatwierdzam, ale w D+3 zmień CTA.\nSpytać Ani o zgodę na promo."
    row = dict(TEMPLATE_ROW, approval_notes=human)
    sb, table = make_sb(dep.TEMPLATES_TABLE, [row])
    wc = make_wc(upsert_template=AsyncMock(side_effect=WintactError(
        MULTILINE_BODY, **PERMANENT_ERROR,
    )))

    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=wc):
        await dep.deploy_approved_templates({})

    [written] = table.updates
    notes = written["approval_notes"]
    marker_lines = [ln for ln in notes.splitlines() if ln.startswith("[deploy-failed")]
    assert len(marker_lines) == 1
    assert "\n" not in marker_lines[0]
    assert '"fields": [ "email.subject" ]' in marker_lines[0]   # zwinięte, nie ucięte
    assert dep._notes_without_marker(notes) == human            # człowiek nietknięty
    assert notes.splitlines()[:2] == human.splitlines()


async def test_repeated_permanent_failures_do_not_accumulate_garbage():
    """Trzy próby = nadal jedna linia markera i jedna notatka człowieka."""
    human = "Nie ruszać przed konsultacją z Anią."
    notes = human
    wc = make_wc(upsert_template=AsyncMock(side_effect=WintactError(
        MULTILINE_BODY, **PERMANENT_ERROR,
    )))

    for _ in range(3):
        sb, table = make_sb(dep.TEMPLATES_TABLE, [dict(TEMPLATE_ROW, approval_notes=notes)])
        with patch.object(dep, "make_supabase_client", return_value=sb), \
             patch.object(dep, "WintactClient", return_value=wc):
            await dep.deploy_approved_templates({})
        [written] = table.updates
        notes = written["approval_notes"]

    assert notes.count("[deploy-failed") == 1
    assert notes.count(dep._DEPLOY_LOG_SEPARATOR) == 1
    assert len(notes.splitlines()) == 3          # człowiek + separator + marker
    assert dep._notes_without_marker(notes) == human


def test_operator_note_shaped_like_our_marker_is_not_eaten():
    """Operator skopiował do notatki linię z logu. To nadal JEGO tekst —
    nie wolno go skasować przy zapisie licznika."""
    human = "[deploy-failed 2/3] skopiowane z logu, pytanie do Alka\ndruga linia"
    notes = dep._notes_with_attempt(human, 1, "WintactError: 400")

    assert dep._failed_attempts(notes) == 1
    assert dep._notes_without_marker(notes) == human


def test_attempt_marker_roundtrip_keeps_operator_note():
    notes = dep._notes_with_attempt("notatka operatora", 1, "WintactError: 500")
    assert dep._failed_attempts(notes) == 1
    assert notes.splitlines()[0] == "notatka operatora"
    assert dep._notes_without_marker(notes) == "notatka operatora"

    notes2 = dep._notes_with_attempt(notes, 2, "WintactError: 500")
    assert dep._failed_attempts(notes2) == 2
    assert notes2.count("[deploy-failed") == 1          # marker się nie multiplikuje
    assert notes2.count(dep._DEPLOY_LOG_SEPARATOR) == 1
    assert dep._notes_without_marker(notes2) == "notatka operatora"


def test_fresh_row_has_zero_attempts():
    assert dep._failed_attempts(None) == 0
    assert dep._failed_attempts("zwykła notatka") == 0


async def test_successful_deploy_clears_stale_attempt_marker():
    stale = f"notatka\n{dep._DEPLOY_LOG_SEPARATOR}\n[deploy-failed 1/3] WintactError: 500"
    row = dict(TEMPLATE_ROW, approval_notes=stale)
    sb, table = make_sb(dep.TEMPLATES_TABLE, [row])
    wc = make_wc(upsert_template=AsyncMock(
        return_value={"id": "d0_cold", "mode": "created", "raw": {}},
    ))

    with patch.object(dep, "make_supabase_client", return_value=sb), \
         patch.object(dep, "WintactClient", return_value=wc):
        out = await dep.deploy_approved_templates({})

    assert out["deployed"] == 1
    [written] = table.updates
    assert written["approval_notes"] == "notatka"
