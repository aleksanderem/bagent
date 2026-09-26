"""Klasyfikacja usługi w krokach po drzewie (drzewo.py) — TypeSafe odpowiada,
kod decyduje.

Krok 1 — rodzina: jeden wybór spośród RODZINY. Typ salonu to kontekst, nie
         odpowiedź (salon masażu sprzedaje depilację).
Krok 2 — rodzaj GŁÓWNY: wybór spośród rodzajów rodziny (+ „inny”). W tym
         samym wywołaniu (pytania spekulatywne, oceniane niezależnie):
         „czy to zestaw kilku zabiegów?” i tak/nie „czy zawiera też X” dla
         każdego rodzaju — kod bierze je pod uwagę TYLKO gdy zestaw ≥ 0,5.
         v1 pytał o każdy rodzaj osobno tak/nie: pytanie tak/nie mierzy
         dopasowanie w oderwaniu od konkurentów, więc „Oczyszczanie wodorowe”
         dostawało 0,39 dla trądziku i 0,32 dla oczyszczania wodorowego, a 21%
         usług nie dostało żadnego rodzaju. Wybór porównuje opcje ze sobą.
         Gdy rodzina niepewna (zwycięzca < PEWNA_RODZINA), krok 2 idzie też
         dla drugiej rodziny — „confidence routing” z dokumentacji.
Wynik: rodzina, rozkład rodzin i zbiór rodzajów.

Progi z dokumentacji TypeSafe, nie strojone: 0,5 = neutralny punkt tak/nie;
0,8 = przykładowy próg działania z „route on uncertainty”.
"""

from __future__ import annotations

import asyncio
from typing import Any

from typesafe_sdk import Choice, Noul, NoulCriteria

from services.typesafe_profile.pytania import build_questions, profile

from .drzewo import RODZINY, WERSJA

MODEL = "jev-1.13.0"
# Wersja pytań klasyfikacji (niezależna od wersji treści drzewa).
KLASYFIKACJA_WERSJA = 4
# v4: stan usługi = nazwa + opis + kategoria + typ salonu + zabieg wybrany przez
# salon w Booksy; osie wariantu (bateria typesafe_profile) liczone w tym samym
# wywołaniu z tej samej pełnej informacji. Cena i czas NIE idą do modelu —
# porównuje je kod (porownanie.py).
OPIS_MAX = 800
U = "`usluga`"
TAK_RODZAJ = 0.5
PEWNA_RODZINA = 0.8
BEAM = 3            # szerokość wiązki (cookbook: K=3)
MIN_KRAWEDZ = 0.05  # rodzina poniżej tego nie wchodzi do wiązki (oszczędność tokenów)
ROWNOLEGLE = 6


def stan(nazwa: str, kategoria: str | None, typ_salonu: str | None,
         opis: str | None = None, zabieg_booksy: str | None = None) -> dict[str, Any]:
    u: dict[str, Any] = {"nazwa": nazwa}
    if kategoria:
        u["kategoria_w_cenniku"] = kategoria
    if typ_salonu:
        u["typ_salonu"] = typ_salonu
    if zabieg_booksy:
        u["zabieg_wybrany_przez_salon_w_booksy"] = zabieg_booksy
    if opis and opis.strip():
        u["opis"] = " ".join(opis.split())[:OPIS_MAX]
    return {"usluga": u}


def _bateria_osi() -> dict[str, Any]:
    """Osie wariantu z baterii destylacji (bez „metoda” — rodzaj daje drzewo;
    „dodatek” baterii = czy usługa sama jest dodatkiem, zostaje pod kluczem ob. baterii)."""
    q = build_questions("")
    q.pop("metoda", None)
    return q


BATERIA = _bateria_osi()


PYTANIE_RODZINA = {
    "rodzina": Choice(
        instructions=(
            f"Do jakiej rodziny zabiegów należy usługa {U}? Rozstrzyga nazwa i kategoria w cenniku; "
            "typ salonu to tylko kontekst — salon może sprzedawać usługi z innej rodziny niż jego typ."
        ),
        criteria={k: {"what": r["opis"], "not_for": r["nie"]} for k, r in RODZINY.items()},
    )
}


PYTANIA_WSPOLNE: dict[str, Any] = {
    "zestaw": Noul(
        instructions=(
            f"Czy usługa {U} to zestaw dwóch lub więcej różnych zabiegów sprzedawanych razem "
            "(np. strzyżenie i broda, henna i regulacja brwi, manicure i pedicure)?"
        ),
        criteria=NoulCriteria(
            true="nazwa wymienia co najmniej dwa różne zabiegi",
            false="jeden zabieg, nawet z dodatkami jak mycie, maska, zdobienie w cenie",
        ),
    ),
    # Dodatek w nazwie = usługa powiązana, nie ta sama (decyzja Alexa 25.09.2026).
    "rozszerzenie": Noul(
        instructions=(
            f"Czy nazwa usługi {U} wymienia dodatek lub rozszerzenie ponad podstawowy zabieg, "
            "np. french, zdobienie, drugi preparat, maska, wzmocnienie, wersja premium lub exclusive?"
        ),
        criteria=NoulCriteria(
            true="nazwa dokłada coś do podstawowego zabiegu",
            false="sam podstawowy zabieg; obszar, długość czy etap to nie dodatek",
        ),
    ),
}


def pytania_rodzajow(rodzina: str) -> dict[str, Any]:
    rodzaje = RODZINY[rodzina]["rodzaje"]
    if not rodzaje:
        return {}
    q: dict[str, Any] = {
        f"glowny:{rodzina}": Choice(
            instructions=f"Jaki jest główny zabieg w usłudze {U}?",
            criteria={
                **{k: {"what": co, "not_for": nie or ""} for k, (_g, co, nie) in rodzaje.items()},
                "inny": {"what": "zabieg spoza tej listy", "not_for": "zabieg opisany którąś z pozostałych opcji"},
            },
        ),
    }
    for key, (_g, co, _nie) in rodzaje.items():
        q[f"zawiera:{key}"] = Noul(instructions=f"Czy usługa {U} zawiera zabieg: {co}?")
    return q


async def klasyfikuj(client: Any, nazwa: str, kategoria: str | None, typ_salonu: str | None,
                     opis: str | None = None, zabieg_booksy: str | None = None) -> tuple[dict[str, Any], int]:
    """Jedna usługa → (klasyfikacja, zużyte tokeny wejścia). Dwa wywołania.

    v3 — przeszukiwanie wiązkowe jak w cookbooku hierarchical_classification:
    wybór rodzaju głównego zadajemy od razu dla BEAM najbardziej prawdopodobnych
    rodzin (pytania spekulatywne w jednym wywołaniu), ścieżkę wybiera średnia
    geometryczna prawdopodobieństw krawędzi rodzina→rodzaj. Błąd na poziomie
    rodziny naprawia pewniejszy rodzaj w drugiej rodzinie.
    """
    st = stan(nazwa, kategoria, typ_salonu, opis, zabieg_booksy)
    r1 = await client.system_one(st, PYTANIE_RODZINA, model=MODEL)
    tokeny = r1.usage.input_tokens or 0
    rc = r1.choices["rodzina"]
    rozklad = sorted(rc.probabilities.items(), key=lambda kv: -kv[1])
    wiazka = [(k, p) for k, p in rozklad[:BEAM] if p >= MIN_KRAWEDZ and RODZINY[k]["rodzaje"]]
    pyt: dict[str, Any] = {**PYTANIA_WSPOLNE, **BATERIA}
    for rodzina, _p in wiazka:
        pyt.update(pytania_rodzajow(rodzina))
    rodzaje: dict[str, float] = {}
    sciezki: list[list] = []
    nouls: dict[str, float] = {}
    osie: dict[str, Any] = {}
    r2 = await client.system_one(st, pyt, model=MODEL)
    tokeny += r2.usage.input_tokens or 0
    nouls = {k: round(a.noul, 3) for k, a in r2.nouls.items()}
    osie = {k: v for k, v in profile(r2).items() if not k.startswith(("zawiera:", "zestaw", "rozszerzenie"))}
    if wiazka:
        for rodzina, p_rodz in wiazka:
            g = r2.choices[f"glowny:{rodzina}"]
            p_rodzaj = g.probabilities.get(g.choice, 0.0)
            sciezki.append([rodzina, g.choice, round((p_rodz * p_rodzaj) ** 0.5, 3), round(g.confidence, 3)])
        sciezki.sort(key=lambda x: -x[2])
        rodzina_w, glowny_w = sciezki[0][0], sciezki[0][1]
        if glowny_w != "inny":
            rodzaje[glowny_w] = sciezki[0][2]
            if nouls.get("zestaw", 0) >= TAK_RODZAJ:
                for key in RODZINY[rodzina_w]["rodzaje"]:
                    if nouls.get(f"zawiera:{key}", 0) >= TAK_RODZAJ:
                        rodzaje.setdefault(key, nouls[f"zawiera:{key}"])
    rodzina = sciezki[0][0] if sciezki else rc.choice
    return {
        "wersja": WERSJA,
        "klasyfikacja_wersja": KLASYFIKACJA_WERSJA,
        "rodzina": rodzina,
        "rodzina_pewnosc": round(rc.probabilities.get(rodzina, 0.0), 3),
        "rodziny_top": [[k, round(p, 3)] for k, p in rozklad[:3]],
        "sciezki": sciezki,
        "zestaw": nouls.get("zestaw"),
        "rozszerzenie": nouls.get("rozszerzenie"),
        "osie": osie,
        "rodzaje_p": rodzaje,
        "rodzaje": sorted(rodzaje),
    }, tokeny


async def klasyfikuj_wiele(client: Any, uslugi: dict[str, tuple]) -> tuple[dict[str, dict], int]:
    """klucz → (nazwa, kategoria, typ[, opis, zabieg_booksy]). Błąd jednej usługi = brak wyniku."""
    sem = asyncio.Semaphore(ROWNOLEGLE)
    out: dict[str, dict] = {}
    tok = [0]

    async def jedna(k: str, *args: Any) -> None:
        async with sem:
            try:
                wynik, t = await klasyfikuj(client, *args)
            except Exception as e:  # noqa: BLE001
                print(f"klasyfikacja {str(args[0])[:40]!r}: {type(e).__name__}: {str(e)[:120]}")
                return
        out[k] = wynik
        tok[0] += t

    await asyncio.gather(*[jedna(k, *v) for k, v in uslugi.items()])
    return out, tok[0]
