"""Testy SLO probe monitoringu — probe_monitoring_alerts_ingesting.

Weryfikuje logikę wykrywania regresji 2026-07-20 (scrape_refresh child-inserts
timeoutują na 109 GB salon_scrape_services → brak alertów → pusty Przegląd).
Mock klienta zwraca count per kolejne .execute() w kolejności, w jakiej próba
odpytuje: [timeouts_last_1h, done_last_2h, queued]. Próg systemowy: >20 świeżych
timeoutów/h = FAIL (pojedyncze gigantyczne salony tolerowane).
"""
import asyncio
from types import SimpleNamespace

from workers.slo_probes import probe_monitoring_alerts_ingesting


class _FakeQuery:
    def __init__(self, counts):
        self._counts = counts

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def gte(self, *a, **k):
        return self

    def lte(self, *a, **k):
        return self

    def ilike(self, *a, **k):
        return self

    def execute(self):
        return SimpleNamespace(count=next(self._counts))


class _FakeClient:
    """Zwraca count z listy po kolei — próba odpytuje 3 razy w stałej kolejności."""

    def __init__(self, counts):
        self._counts = iter(counts)

    def table(self, _name):
        return _FakeQuery(self._counts)


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
