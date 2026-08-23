"""Testy sond SLO — probe_monitoring_alerts_ingesting i probe_competitor_report_subject_only.

`probe_monitoring_alerts_ingesting` weryfikuje logikę wykrywania regresji
2026-07-20 (scrape_refresh child-inserts timeoutują na 109 GB
salon_scrape_services → brak diffów → brak alertów → pusty Przegląd). Próg
systemowy: >20 świeżych timeoutów/h = FAIL (pojedyncze gigantyczne salony
tolerowane).

`probe_competitor_report_subject_only` weryfikuje WYBÓR KANARKA
(BEAUTY_AUDIT-rxoq): kanarkiem ma być NAJŚWIEŻSZY ukończony raport, a nie ten
o najwyższym id, plus escape na zastój („brak świeżego raportu w oknie" =
PASS z jawnym detalem, nie cisza nieodróżnialna od awarii).

Atrapa klienta ma dwa tryby, bo obie sondy potrzebują czego innego —
szczegóły przy klasie `_FakeQuery`.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from workers.slo_probes import (
    probe_competitor_report_subject_only,
    probe_monitoring_alerts_ingesting,
)


class _FakeQuery:
    """Atrapa buildera PostgREST. Dwa tryby, wybierane przez `_FakeClient`.

    TRYB KOLEJKI (`rows is None`) — kolejne `.execute()` zwracają count z listy,
    w kolejności odpytywania. Wystarcza sondom, które tylko liczą wiersze
    (probe_monitoring_alerts_ingesting).

    TRYB TABELI (`rows` = lista dictów) — `.eq/.gte/.lte/.ilike/.order/.limit`
    są NAPRAWDĘ wykonywane na wierszach. To nie jest ozdobnik: gdyby atrapa
    ignorowała `.order()`, test na wybór kanarka po świeżości przechodziłby tak
    samo przy `order("updated_at")` jak przy `order("id")` — czyli byłby
    wydmuszką. Tutaj przywrócenie sortowania po id zmienia zwracany wiersz i
    test pada.

    Porównania na timestampach są leksykograficzne na stringach ISO. Jest to
    poprawne, bo i sonda (`_iso_ago`), i testy (`_ts_ago`) produkują ten sam
    format w UTC.
    """

    def __init__(self, rows, responses):
        self._rows = rows
        self._responses = responses
        self._filters: list[tuple[str, str, object]] = []
        self._order: tuple[str, bool] | None = None
        self._limit: int | None = None

    def select(self, *a, **k):
        return self

    def eq(self, column, value):
        self._filters.append(("eq", column, value))
        return self

    def gte(self, column, value):
        self._filters.append(("gte", column, value))
        return self

    def lte(self, column, value):
        self._filters.append(("lte", column, value))
        return self

    def ilike(self, column, value):
        self._filters.append(("ilike", column, value))
        return self

    def order(self, column, desc=False):
        self._order = (column, desc)
        return self

    def limit(self, n):
        self._limit = n
        return self

    def _matches(self, row) -> bool:
        for op, column, value in self._filters:
            cell = row.get(column)
            if op == "eq" and cell != value:
                return False
            if op == "gte" and (cell is None or cell < value):
                return False
            if op == "lte" and (cell is None or cell > value):
                return False
            if op == "ilike":
                needle = str(value).strip("%").lower()
                if cell is None or needle not in str(cell).lower():
                    return False
        return True

    def execute(self):
        if self._rows is None:
            return next(self._responses)
        rows = [r for r in self._rows if self._matches(r)]
        if self._order is not None:
            column, desc = self._order
            rows = sorted(rows, key=lambda r: r[column], reverse=desc)
        if self._limit is not None:
            rows = rows[: self._limit]
        return SimpleNamespace(data=rows, count=len(rows))


def _as_response(item):
    """int → odpowiedź samym countem (stary kształt atrapy)."""
    if isinstance(item, int):
        return SimpleNamespace(count=item, data=None)
    return item


class _FakeClient:
    """`counts` = tryb kolejki, `tables` = tryb tabeli (patrz `_FakeQuery`)."""

    def __init__(self, counts=None, tables=None):
        self._responses = iter([_as_response(c) for c in (counts or [])])
        self._tables = tables or {}

    def table(self, name):
        if name in self._tables:
            return _FakeQuery(self._tables[name], self._responses)
        return _FakeQuery(None, self._responses)


# ---------------------------------------------------------------------------
# probe_monitoring_alerts_ingesting
# ---------------------------------------------------------------------------


def _run(counts):
    return asyncio.run(probe_monitoring_alerts_ingesting(_FakeClient(counts)))


def test_healthy_ingest_passes():
    # timeouts_1h=0, done=5 (są udane ingesty), queued=10 → OK
    res = _run([0, 5, 10])
    assert res.ok is True
    assert "timeouts_last_1h=0" in res.detail


def test_statement_timeout_regression_fails():
    # 50 świeżych timeoutów/h (>20 próg) → systemowa regresja → FAIL
    res = _run([50, 0, 30])
    assert res.ok is False
    assert "timeouts_last_1h=50" in res.detail


def test_few_stragglers_below_threshold_pass():
    # 5 timeoutów/h (pojedyncze gigantyczne salony) < próg 20, są completions → OK
    res = _run([5, 8, 10])
    assert res.ok is True


def test_no_work_does_not_false_alarm():
    # pusta kolejka, zero completions → NIE alarmuj (brak pracy != awaria)
    res = _run([0, 0, 0])
    assert res.ok is True


def test_queued_but_no_completions_fails():
    # jest praca (queued=50) ale zero completed w 2h i brak timeoutów → FAIL
    # (ingest cicho nie kończy — coś stoi)
    res = _run([0, 0, 50])
    assert res.ok is False
    assert "done_last_2h=0" in res.detail


# ---------------------------------------------------------------------------
# probe_competitor_report_subject_only — wybór kanarka (BEAUTY_AUDIT-rxoq)
# ---------------------------------------------------------------------------


def _ts_ago(hours: float) -> str:
    """Znacznik czasu w formacie, jaki produkuje `_iso_ago` w sondzie."""
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _pricing_rows(report_id: int, total: int, subject_only: int) -> list[dict]:
    """`total` wierszy wyceny raportu, z czego `subject_only` bez bliźniaka."""
    return [
        {
            "report_id": report_id,
            "verification_status": (
                "subject_only" if i < subject_only else "twin_verified"
            ),
        }
        for i in range(total)
    ]


def _run_subject_only(reports: list[dict], pricing: list[dict]):
    client = _FakeClient(tables={
        "competitor_reports": reports,
        "competitor_pricing_comparisons": pricing,
    })
    return asyncio.run(probe_competitor_report_subject_only(client))


def test_canary_is_freshest_report_not_highest_id():
    """Stary zdegenerowany raport o WYŻSZYM id nie może przykrywać świeżego zdrowego.

    Dokładnie układ z produkcji 2026-08-23: raport 250 ma wyższe id, ale 222
    przeliczono PÓŹNIEJ. Wersja sondy sortująca po id oceniłaby 250 (24/24) i
    zwróciła FAIL mimo zdrowego ostatniego przebiegu.
    """
    reports = [
        {"id": 250, "status": "completed", "updated_at": _ts_ago(6)},
        {"id": 222, "status": "completed", "updated_at": _ts_ago(1)},
    ]
    pricing = _pricing_rows(250, 24, 24) + _pricing_rows(222, 20, 4)

    res = _run_subject_only(reports, pricing)

    assert res.ok is True
    assert "report=222" in res.detail
    assert "pct=20.0%" in res.detail


def test_stale_reports_pass_with_explicit_detail():
    """Zastój (brak raportu w oknie) = PASS z jawnym detalem, nie cicha zieleń.

    Bez tego escape'u zdegenerowany raport sprzed tygodni trzymałby sondę w
    DOWN w nieskończoność, mimo że nic nie mówi o dzisiejszym silniku.
    """
    reports = [{"id": 250, "status": "completed", "updated_at": _ts_ago(200)}]
    pricing = _pricing_rows(250, 24, 24)

    res = _run_subject_only(reports, pricing)

    assert res.ok is True
    assert "stale_no_recent_report" in res.detail
    assert "newest_report=250" in res.detail
    assert "age=200h" in res.detail
    assert "window=72h" in res.detail


def test_fresh_degenerate_report_still_fails():
    """Sonda dalej ma alarmować, gdy powinna — świeży raport 100% subject_only."""
    reports = [{"id": 300, "status": "completed", "updated_at": _ts_ago(1)}]
    pricing = _pricing_rows(300, 24, 24)

    res = _run_subject_only(reports, pricing)

    assert res.ok is False
    assert "report=300 subject_only=24/24 pct=100.0%" in res.detail


def test_production_state_2026_08_23_keeps_alerting():
    """Naprawa NIE wycisza dzisiejszego alertu — i o to chodzi.

    Oba raporty (250 i 222) mają 24/24 subject_only. Po zmianie kanarkiem jest
    222 (przeliczony później), więc sonda dalej świeci na czerwono, dopóki
    silnik wyceny jest zepsuty.
    """
    reports = [
        {"id": 250, "status": "completed", "updated_at": _ts_ago(13.0)},
        {"id": 222, "status": "completed", "updated_at": _ts_ago(12.75)},
    ]
    pricing = _pricing_rows(250, 24, 24) + _pricing_rows(222, 24, 24)

    res = _run_subject_only(reports, pricing)

    assert res.ok is False
    assert "report=222 subject_only=24/24 pct=100.0%" in res.detail


def test_no_completed_reports_passes():
    """Raport w trakcie generowania nie jest kanarkiem — nie ma czego oceniać."""
    reports = [{"id": 1, "status": "processing", "updated_at": _ts_ago(1)}]

    res = _run_subject_only(reports, [])

    assert res.ok is True
    assert res.detail == "no_completed_reports"


def test_report_without_pricing_rows_passes():
    """Świeży raport bez wierszy wyceny → pass, nie dzielenie przez zero."""
    reports = [{"id": 301, "status": "completed", "updated_at": _ts_ago(2)}]

    res = _run_subject_only(reports, [])

    assert res.ok is True
    assert "report=301 no_pricing_rows" in res.detail
