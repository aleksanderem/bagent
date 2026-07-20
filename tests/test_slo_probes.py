"""Testy SLO probe monitoringu — probe_monitoring_alerts_ingesting.

Weryfikuje logikę wykrywania regresji 2026-07-20 (scrape_refresh child-inserts
timeoutują na 109 GB salon_scrape_services → brak alertów → pusty Przegląd).
Mock klienta zwraca count per kolejne .execute() w kolejności, w jakiej próba
odpytuje: [stuck_timeout, done_last_2h, queued].
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
    # stuck=0, done=5 (są udane ingesty), queued=10 → OK
    res = _run([0, 5, 10])
    assert res.ok is True
    assert "stuck_timeout=0" in res.detail


def test_statement_timeout_regression_fails():
    # stuck=6 (joby kręcą się w requeue na timeout) → FAIL, niezależnie od reszty
    res = _run([6, 0, 6])
    assert res.ok is False
    assert "stuck_timeout=6" in res.detail


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
