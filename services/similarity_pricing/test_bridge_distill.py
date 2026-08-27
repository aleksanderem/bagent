"""Pomost destylacji przy raporcie (2026-08-27) — kontrakt.

Docelowo destyluje ingest scrape'ów; pomost łata dziury i po wdrożeniu ingest
zostaje siatką bezpieczeństwa (każda destylacja pomostu zapisuje się trwale,
więc miss rate maleje do zera sam).
"""
import asyncio
import json

import services.similarity_pricing.report_pricing as rp


class _FakeGLM:
    def __init__(self):
        self.calls = 0

        class _Chat:
            def __init__(self, outer): self._o = outer

            @property
            def completions(self): return self

            async def create(self, **kw):
                self._o.calls += 1
                tresc = kw["messages"][-1]["content"]
                poz = json.loads(tresc[tresc.index("POZYCJE:") + 9:])
                wynik = {"result": [{"i": p["i"], "osie": {"metoda": "masaż", "obszar": "plecy"}} for p in poz]}

                class _R: pass
                r = _R(); ch = _R(); msg = _R()
                msg.content = json.dumps(wynik); ch.message = msg; r.choices = [ch]
                return r
        self.chat = _Chat(self)


class _FakeDB:
    def __init__(self): self.upserty = []

    def table(self, name):
        self._t = name; return self

    def upsert(self, rows, on_conflict=None):
        self.upserty.append((self._t, list(rows), on_conflict)); return self

    def execute(self): return self


class _FakeService:
    def __init__(self): self.client = _FakeDB()


def _run(coro): return asyncio.get_event_loop().run_until_complete(coro)


def test_pomost_destyluje_i_zapisuje_trwale(monkeypatch):
    fake = _FakeGLM()
    monkeypatch.setattr(rp, "_bridge_glm_client", lambda: fake)
    svc = _FakeService()
    clusters = {1: [{"service_name": "Masaż pleców 30 min"}]}
    subject = [{"name": "Masaż pleców"}]
    nowe = _run(rp._bridge_distill_missing(svc, "Masaż", {}, subject, clusters))
    assert set(nowe) == {"masaż pleców", "masaż pleców 30 min"}
    assert fake.calls == 1
    # zapis TRWAŁY — pomost jest przyrostowym backfillem
    (tabela, rows, klucz), = svc.client.upserty
    assert tabela == "service_taxonomy" and klucz == "name_key,branza"
    assert all(r["branza"] == "Masaż" and r["model"] == rp._BRIDGE_MODEL for r in rows)


def test_pomost_nie_dotyka_terytorium_m3(monkeypatch):
    """Model purity (mig 189): wartości GLM w branżach M3 = fałszywe weta 3-14%."""
    fake = _FakeGLM()
    monkeypatch.setattr(rp, "_bridge_glm_client", lambda: fake)
    for branza in sorted(rp._BRIDGE_M3_BRANZE):
        wynik = _run(rp._bridge_distill_missing(_FakeService(), branza, {}, [{"name": "X"}], {}))
        assert wynik == {}
    assert fake.calls == 0


def test_pomost_bez_klucza_milczy(monkeypatch):
    monkeypatch.setattr(rp, "_bridge_glm_client", lambda: None)
    assert _run(rp._bridge_distill_missing(_FakeService(), "Masaż", {}, [{"name": "X"}], {})) == {}


def test_pomost_pomija_nazwy_juz_zdestylowane(monkeypatch):
    fake = _FakeGLM()
    monkeypatch.setattr(rp, "_bridge_glm_client", lambda: fake)
    juz = {"masaż pleców": {"metoda": "masaż"}}
    nowe = _run(rp._bridge_distill_missing(_FakeService(), "Masaż", juz, [{"name": "Masaż pleców"}], {}))
    assert nowe == {} and fake.calls == 0


def test_pomost_blad_nie_wywraca_raportu(monkeypatch):
    class _Wybuch:
        @property
        def chat(self): raise RuntimeError("boom")
    monkeypatch.setattr(rp, "_bridge_glm_client", lambda: _Wybuch())
    assert _run(rp._bridge_distill_missing(_FakeService(), "Masaż", {}, [{"name": "X"}], {})) == {}
