"""Warstwa COHERENCE GUARD — uniwersalna detekcja OBCYCH BLOKÓW w klastrze.

CO
==
Filtr klastra bliźniaków oparty wyłącznie na GEOMETRII przestrzeni embeddingów,
bez jakiejkolwiek wiedzy o nazwach/domenach usług. Wykrywa i odrzuca "obce
bloki": grupy sampli, które są wyraźnie bliżej SIEBIE NAWZAJEM niż subjectu.

Sygnatura obcego bloku (per sample):
    peer_max_sim − similarity > gap    ORAZ    similarity < s_max

gdzie:
    similarity    — cosine podobieństwo sampla do SUBJECTU (z RPC
                    fn_find_related_v2; to na jego podstawie sample wszedł
                    do klastra),
    peer_max_sim  — max cosine podobieństwo sampla do INNEGO sampla klastra
                    (RPC fn_pairwise_max_similarity, mig 151; liczone w
                    Postgresie, wstrzykiwane do sampli w report_pricing).

PO CO
=====
Sweep 2026-07-19 (250 losowych subjectów, pełna produkcyjna ścieżka geo→RPC→
engine) wykazał, że 20% subjectów z policzonym rynkiem miało w kept-klastrze
obcy blok przesuwający medianę o ≥10% (mediana wpływu 5.2%, p90 25%, max
+112%). Twarde przykłady z prod:
  * "Bikini linią"        ← 52×"Bikini płytkie"          (impact +84.6%)
  * "Manicure hybrydowy"  ← 29×"Pedicure hybrydowy"      (raport #250, verified)
  * "Uzupełnienie rzęs 2:1" ← 13×"Uzupełnienie rzęs 1:1" (wolumen)

Dlaczego istniejące warstwy tego nie łapały:
  * embedding = f(nazwa+opis) → pasmo similarity prawdziwych bliźniaków
    (0.82–1.0) NAKŁADA SIĘ na pasmo obcych usług o zbieżnym słownictwie
    (mani↔pedi 0.834–0.841) — sam próg NIGDY tego nie rozdzieli;
  * osie tożsamości (layer_identity) głosują na metadanych — gdy kategoria
    właściciela jest pusta/personalna ("Stylistka Zuzia"), a nazwa nie niesie
    rozpoznawalnych parametrów, WSZYSTKIE osie się wstrzymują;
  * metryka czystości adaptacyjnej surowości używa tych samych osi, więc
    "czystość" wychodzi 1.0 przy 29 obcych samplach — filtr nie eskaluje.

Geometria wypełnia dokładnie tę lukę: obcy blok ZAWSZE zdradza się spójnością
wewnętrzną (te same usługi u wielu salonów → wzajemne sim ~1.0) przy niższym
sim do subjectu. Prawdziwe bliźniaki mają peer_max_sim ≈ similarity (gap ~0).

Kalibracja progów NA REALNYCH DANYCH (histogram gap z 6.7k sampli raportów):
masa prawdziwych bliźniaków siedzi w gap ≤ +0.05; obce bloki od ~+0.10 wzwyż.
gap=0.08 z marginesem; s_max=0.90 chroni sample de-facto tożsame z subjectem
(similarity ≥ 0.90 = to samo nazwa+opis-blisko, nie ruszamy niezależnie od
struktury bloków — obniża false-positives na "artefakcie opisu", patrz niżej).

ZNANE OGRANICZENIE (artefakt opisu): embedding zawiera opis usługi, więc
identyczna nazwa z innym opisem potrafi mieć similarity 0.82–0.88 do subjectu
przy peer_max_sim ~1.0 (sweep: 16/103 flag = ta klasa). Dlatego warstwa
odrzuca TYLKO gdy dodatkowo similarity < s_max, a wątpliwe przypadki zostawia
sufficiency/cenie. Świadomy trade-off: wolimy przepuścić artefakt (cena i tak
z tej samej usługi) niż wyciąć prawdziwe bliźniaki.

GDZIE W PIPELINE
================
engine.compute_market_price:
    identity (Drugie prawo) → **COHERENCE (ta warstwa)** → dedup → sufficiency
    → unit/cena.
Po identity (żeby nie dublować twardych wet), przed dedup (blok widać w pełnej
masie — dedup per salon mógłby zredukować blok do pojedynczych sampli i osłabić
sygnał min_block).

Brak pola peer_max_sim na samplu = ABSTAIN (sample nietykany). Dzięki temu:
  * engine pozostaje pure i w pełni testowalny offline,
  * deploy-order-safe: stary report_pricing (bez wstrzykiwania) → warstwa
    przezroczysta; embeddingi zmiecione przez mig 149 → abstain, nie crash.

DLACZEGO NIE (alternatywy odrzucone):
  * słownik nazw (manicure→dłonie itd.) — łata per przypadek, sprzeczna z
    filozofią silnika; następna kolizja (Bikini linią/płytkie) i tak by weszła;
  * clustering (k-medoid/HDBSCAN) na klastrze — cięższe, niedeterministyczne
    progi, trudne do wyjaśnienia w provenance; para (s_i, m_i) wystarcza;
  * podnoszenie progu RPC 0.82→0.9 — zabija recall na rzadkich rynkach
    (fallback 0.75 istnieje właśnie po to, by go nie zabijać).
"""

from __future__ import annotations

from typing import Any

# Progi skalibrowane empirycznie (sweep 2026-07-19, patrz docstring modułu).
# Trzymane jako stałe modułu + nadpisywalne przez config engine'u — spójnie
# z konwencją pozostałych warstw (meta-pokrętła w DEFAULT_CONFIG).
DEFAULT_GAP = 0.08          # peer_max_sim - similarity powyżej tego = podejrzany
DEFAULT_S_MAX = 0.90        # similarity >= s_max nigdy nie jest odrzucane
DEFAULT_MIN_BLOCK = 2       # min liczba podejrzanych sampli, by uznać BLOK
                            # (pojedynczy sample bez "brata" w klastrze ma niskie
                            # peer_max_sim, więc i tak się nie flaguje; min_block
                            # chroni przed odrzuceniem 1-2 przypadkowych outlierów,
                            # które sufficiency/dedup i tak zneutralizują)


def drop_foreign_blocks(
    samples: list[dict[str, Any]],
    *,
    gap: float = DEFAULT_GAP,
    s_max: float = DEFAULT_S_MAX,
    min_block: int = DEFAULT_MIN_BLOCK,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Odrzuć z klastra obce bloki (geometria: bliżej siebie niż subjectu).

    Args:
        samples: kept-klaster po teście tożsamości. Wykorzystywane pola sampla:
            similarity (float, wymagane do oceny), peer_max_sim (float|None,
            brak => abstain), service_name (tylko do meta/diagnostyki).
        gap/s_max/min_block: progi — patrz stałe modułu.

    Returns:
        (kept, meta). Nie mutuje wejścia (konwencja modułu: immutable).
        meta trafia do MarketResult.provenance["coherence"] — pełna
        wyjaśnialność każdej decyzji (co odrzucono i dlaczego).
    """
    suspects: list[dict[str, Any]] = []
    abstained = 0
    for s in samples:
        sim = s.get("similarity")
        pm = s.get("peer_max_sim")
        if sim is None or pm is None:
            abstained += 1
            continue
        if float(pm) - float(sim) > gap and float(sim) < s_max:
            suspects.append(s)

    if len(suspects) >= min_block:
        dropped_ids = {id(s) for s in suspects}
        kept = [s for s in samples if id(s) not in dropped_ids]
        dropped = suspects
    else:
        kept, dropped = list(samples), []

    meta = {
        "gap": gap,
        "s_max": s_max,
        "min_block": min_block,
        "n_in": len(samples),
        "n_abstained_no_peer_sim": abstained,
        "n_suspect": len(suspects),
        "n_dropped": len(dropped),
        # Nazwy odrzuconych — WYŁĄCZNIE dla wyjaśnialności (provenance/debug);
        # decyzja zapadła geometrycznie, zanim ktokolwiek spojrzał na nazwę.
        "dropped_names": sorted({(s.get("service_name") or "")[:60] for s in dropped})[:10],
        "dropped_gap_range": (
            [round(min(float(s["peer_max_sim"]) - float(s["similarity"]) for s in dropped), 3),
             round(max(float(s["peer_max_sim"]) - float(s["similarity"]) for s in dropped), 3)]
            if dropped else None
        ),
    }
    return kept, meta
