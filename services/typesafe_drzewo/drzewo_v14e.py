"""ZAMROŻONE 28.09 — v14e (wersja sprawdzianów 3–4), tylko do bilansu z v14f na tych samych parach. Nie zmieniać.

Drzewo v14 — drzewo do poziomu zabiegu, niżej frazy obu usług porównane wprost (bd BEAUTY_AUDIT-asrk, 28.09).

Zgoda Alexa (28.09, warunkowa): cookbook TypeSafe hierarchical_classification obowiązuje do poziomu ZABIEGU
(dziedzina → zabieg; zabieg z kilkoma rodzicami jak MeSH). Poniżej — metoda, gdzie i ile, etap, dodatki, skład
zestawu — nie wybieramy z listy opcji: lista zawsze była niepełna (ocena 181 par: lasery różnych typów wychodziły
„tą samą”, identyczne pedicure „za mało danych”). Porównujemy frazy, które TypeSafe przypisał słowom każdej usługi
w jej pełnym kontekście (rola słowa) — tak, jak porównuje je człowiek. Trzy rundy pytań:
  0. frazy z kontekstu (zabieg z Booksy, kategoria cennika) uzupełniają tylko poziom, którego usługa sama nie podaje,
     i liczą się dopiero, gdy TypeSafe potwierdzi je na tej usłudze („STYLIZACJA RZĘS I BRWI” nie daje regulacji
     brwi obszaru „rzęs”); fraza zabiegu, która nazywa ten sam zabieg co węzeł, nie jest składem;
  1. identyczna fraza (dowolny poziom, bo role bywają niezgodne: „1:1” raz metoda, raz wielkość) = zgodna; dwie
     różne frazy TEJ SAMEJ roli → TypeSafe tak/nie: czy znaczą to samo (wzór pól z cookbooka entity_alignment);
  2. fraza bez odpowiednika → TypeSafe na pełnym kontekście DRUGIEJ usługi: fraza z nazwy — czy druga też ma tę
     cechę („męskie” w barberze); fraza z wariantu — czy druga obejmuje też ten wariant (rodzina długości włosów
     kontra „cena zależy od długości”). Jeśli nie: druga podaje tę cechę inaczej = znana różnica, nie podaje = za
     mało danych. Dodatek albo dodatkowy zabieg tylko w jednej = podobna (decyzja Alexa 25.09).
Werdykt: wszystko zgodne → ta sama; znana różnica → podobna; jedna podaje, druga nie → za mało danych;
inny zabieg → inna. Kod porównuje tylko identyczne frazy; resztę rozstrzyga TypeSafe. Brak odpowiedzi
TypeSafe nigdy nie daje „ta sama”.
"""

from __future__ import annotations

from typing import Any

from typesafe_sdk import Noul, NoulCriteria

from .drzewo_v13 import INNE, OGOLNIE, POROWNYWANE, pozycja
from .schemat_v10 import TAK_DODATEK, _jednostronne

POZIOM_ROLI = {"metoda": "metoda", "obszar": "gdzie i ile", "wielkosc": "gdzie i ile", "dla_kogo": "gdzie i ile",
               "etap": "etap", "skladnik": "dodatek", "zabieg": "skład"}
POZIOMY = ("metoda", "gdzie i ile", "etap", "dodatek", "skład")
NR = {"metoda": 2, "gdzie i ile": 3, "etap": 4, "dodatek": 5, "skład": 5}
WLASNE = ("nazwa", "wariant")  # opisują samą usługę
KONTEKST = ("zabieg_booksy", "kategoria")  # zabieg z Booksy jest wybierany per usługa, kategoria grupuje kilka usług
Z_KONTEKSTU = frozenset({"metoda", "gdzie i ile", "etap"})
DOMNIEMANE = frozenset({"metoda", "gdzie i ile", "etap"})  # dodatek i skład podane przez jedną = podobna (Alex 25.09)
CECHA, WARIANT, ZMIANA = "cecha", "wariant", "zmiana"
PRZYIMKI = frozenset("do od z ze w we na dla po u bez przy pod nad za przez wraz".split())
CUDZYSLOWY = str.maketrans("", "", "„”“\"'«»")
OPIS_POZIOMU = {"metoda": "metoda", "gdzie i ile": "obszar, wielkość albo dla kogo", "etap": "etap"}
OPIS_ROLI = {"metoda": "metoda, technika, urządzenie albo preparat", "obszar": "obszar albo część ciała, której dotyczy zabieg",
             "wielkosc": "długość, rozmiar, gęstość albo liczba", "dla_kogo": "dla kogo", "etap": "etap w cyklu zabiegu",
             "skladnik": "dodatek w cenie", "zabieg": "zabieg"}
_STAN_USLUGI = "Rozstrzygają nazwa, kategoria, opis, warianty, zabieg wybrany w Booksy i salon `salon`."

Fraza = tuple[str, str, str]           # (rola, fraza, źródło)
FrazaP = tuple[str, str, str, str]     # (poziom, rola, fraza, źródło)
KluczCechy = tuple[int, str, str, str]  # (id usługi, fraza, poziom, CECHA | WARIANT)


def pytanie_tozsamosci(rola: str) -> Noul:
    """Para fraz TEJ SAMEJ roli (cecha wspólna z góry) — czy znaczą to samo. Próba 28.09: pytanie o relację fraz
    dowolnych ról myliło cechy („ramion” kontra „włosy”, „architektura” kontra „brwi” → „inna wartość”)."""
    return Noul(instructions=f"Obie usługi to zabieg `zabieg`. Czy fraza `a` znaczy to samo co fraza `b` — jako {OPIS_ROLI[rola]}?",
                criteria=NoulCriteria(true="to samo, tylko inaczej zapisane: odmiana słowa, skrót, synonim, inna kolejność słów "
                                           "albo inny język",
                                      false="coś innego: inna wartość, węższa albo szersza"))


def pytanie_cechy(fraza: str, poziom: str) -> Noul:
    """Czy usługa ma cechę — i dla frazy z własnego kontekstu (kategoria), i dla frazy podanej przez drugą usługę.
    Próba 28.09: „czy usługa też JEST „dłoni”” dawało nie dla manicure — pytamy o cechę, nie o tożsamość."""
    return Noul(instructions=f"Czy usługa `usluga` ma cechę „{fraza}” ({OPIS_POZIOMU[poziom]}), nawet jeśli nie pisze tego "
                             f"wprost? {_STAN_USLUGI}",
                criteria=NoulCriteria(true=f"tak — z usługi albo salonu wynika, że ma cechę „{fraza}”",
                                      false="nie ma tej cechy albo nie wiadomo"))


def pytanie_wariantu(fraza: str, poziom: str) -> Noul:
    """Fraza z wariantu drugiej usługi (rodzina: długości, obszary, pakiety) — czy ta usługa też ją obejmuje."""
    return Noul(instructions=f"Czy usługa `usluga` obejmuje też wariant „{fraza}” ({OPIS_POZIOMU[poziom]}) — jest dla niego "
                             f"wykonywana za tę cenę albo w jednym ze swoich wariantów? {_STAN_USLUGI}",
                criteria=NoulCriteria(true=f"obejmuje — usługa jest wykonywana także w wariancie „{fraza}”",
                                      false="nie obejmuje albo nie wiadomo"))


def pytanie_zmiany(fraza: str, zabieg: str) -> Noul:
    """Słowo o roli zabiegu w usłudze, która NIE jest zestawem: zmienia zabieg czy tylko go opisuje. Sprawdzian 2
    (28.09): pominięcie takich słów naprawiło „Lip Flip powiększenie ust” i „redukcja / likwidacja uśmiechu
    dziąsłowego”, ale zgubiło „Remover brwi” (usuwanie makijażu permanentnego) i „Odświeżenie brwi”."""
    return Noul(instructions=f"Usługa `usluga` to zabieg „{zabieg}”, a w jej nazwie jest słowo „{fraza}”. Czy „{fraza}” oznacza "
                             f"inny zabieg albo inną wizytę niż sam zabieg „{zabieg}” — np. usuwanie zamiast wykonania, "
                             f"odświeżenie, korektę, drugi zabieg? {_STAN_USLUGI}",
                criteria=NoulCriteria(true=f"tak — „{fraza}” zmienia, co się robi w tej usłudze",
                                      false=f"nie — „{fraza}” tylko opisuje zabieg „{zabieg}”: jego cel, efekt albo inną nazwę"))


def pytanie_dla(klucz: KluczCechy) -> Noul:
    _uid, fraza, poziom, typ = klucz
    if typ == ZMIANA:
        return pytanie_zmiany(fraza, poziom)  # dla ZMIANA trzecie pole to etykieta węzła
    return pytanie_wariantu(fraza, poziom) if typ == WARIANT else pytanie_cechy(fraza, poziom)


def klucz_relacji(z: str, rola: str, a: str, b: str) -> tuple[str, str, str, str]:
    x, y = sorted((a, b))
    return z, rola, x, y


def stan_relacji(klucz: tuple[str, str, str, str]) -> dict[str, str]:
    z, _rola, a, b = klucz
    return {"zabieg": z, "a": a, "b": b}


def _wezel(rek: dict | None) -> tuple[str, str] | None:
    if not rek or not rek.get("zabiegi"):
        return None
    d, g, _p, _r = max(rek["zabiegi"], key=lambda z: z[2])  # remis: pierwszy w kolejności wiązki
    return d, g


def profil_v14(uid: int, frazy: list[Fraza], rek: dict | None, drzewo: dict) -> dict | None:
    """frazy: (rola, fraza, źródło) ze słów usługi; rek: przejście drzewa (poziomy 1–2, pozycja, tak/nie).
    → {"id", "pozycja", "zestaw", "rozszerzenie", "wezel", "etykieta", "frazy": {poziom: [(rola, fraza, źródło)]}}.
    Frazy z kontekstu wymagają potwierdzenia (potwierdz)."""
    if not rek:
        return None
    wezel = _wezel(rek)
    w = drzewo["zabiegi"].get(wezel[1], {}) if wezel else {}
    nazwy_wezla = {w.get("etykieta", ""), *w.get("synonimy", [])} - {""}
    # kontekst uzupełnia każdą CECHĘ (rolę) osobno — sprawdzian 2 (28.09): „damskie” z zabiegu Booksy przepadało,
    # bo nazwa podawała długość, a uzupełnienie działało na cały poziom „gdzie i ile”
    # (suma kategorii i zabiegu z Booksy dokładała szczegóły z kategorii grupy — „laserem diodowym” przy „Uda”)
    zrodla = ("wlasne", *KONTEKST)
    kosze: dict[str, dict[str, list[Fraza]]] = {r: {z: [] for z in zrodla} for r in POZIOM_ROLI}
    for rola, fraza, zr in frazy:
        fraza = " ".join(fraza.translate(CUDZYSLOWY).split())
        kosz = "wlasne" if zr in WLASNE else zr
        if rola in POZIOM_ROLI and kosz in zrodla and fraza and all(x != fraza for _r, x, _z in kosze[rola][kosz]):
            kosze[rola][kosz].append((rola, fraza, zr))
    out: dict[str, list[Fraza]] = {p: [] for p in POZIOMY}
    for rola, p in POZIOM_ROLI.items():
        k = kosze[rola]
        f = k["wlasne"] or (next((k[z] for z in KONTEKST if k[z]), []) if p in Z_KONTEKSTU else [])
        out[p] += [e for e in f if rdzen(e[1]) not in nazwy_wezla] if p == "skład" else f
    return {"id": uid, "pozycja": rek.get("pozycja"), "zestaw": rek.get("zestaw"), "rozszerzenie": rek.get("rozszerzenie"),
            "wezel": wezel, "etykieta": w.get("etykieta") or (wezel[1].split("|")[-1] if wezel else None), "frazy": out}


def rdzen(fraza: str) -> str:
    """Fraza bez przyimka na początku („z regulacją” → „regulacją”) — do porównania z nazwą węzła."""
    slowa = fraza.split()
    return " ".join(slowa[1:]) if len(slowa) > 1 and slowa[0] in PRZYIMKI else fraza


def potrzebne_potwierdzenia(p: dict | None) -> tuple[set[KluczCechy], set[tuple[str, str, str, str]]]:
    """Runda 0 → (frazy z kontekstu do potwierdzenia na tej usłudze, frazy zabiegu do sprawdzenia z węzłem)."""
    if not p:
        return set(), set()
    cechy = {(p["id"], x, poz, CECHA) for poz in Z_KONTEKSTU for _r, x, zr in p["frazy"][poz] if zr in KONTEKST}
    if not p["etykieta"]:
        return cechy, set()
    if _zestaw(p):
        return cechy, {klucz_relacji(p["etykieta"], "zabieg", x, p["etykieta"]) for _r, x, _z in p["frazy"]["skład"]}
    return cechy | {(p["id"], x, p["etykieta"], ZMIANA) for _r, x, _z in p["frazy"]["skład"]}, set()


def potwierdz(p: dict | None, rel: dict, cechy: dict) -> dict | None:
    """Nowy profil: frazy z kontekstu tylko potwierdzone na tej usłudze; bez fraz zabiegu, które nazywają węzeł."""
    if not p:
        return p
    def zostaje(poz: str, x: str, zr: str) -> bool:
        if zr in KONTEKST and cechy.get((p["id"], x, poz, CECHA)) is not True:
            return False
        if poz != "skład" or not p["etykieta"]:
            return True
        if _zestaw(p):  # w zestawie: każdy zabieg poza tym, który nazywa węzeł
            return rel.get(klucz_relacji(p["etykieta"], "zabieg", x, p["etykieta"])) is not True
        return cechy.get((p["id"], x, p["etykieta"], ZMIANA)) is True  # poza zestawem: tylko słowo, które zmienia zabieg
    frazy = {poz: [(r, x, zr) for r, x, zr in lista if zostaje(poz, x, zr)] for poz, lista in p["frazy"].items()}
    return {**p, "frazy": frazy}


def _zestaw(x: dict) -> bool:
    return (x.get("zestaw") or 0) >= TAK_DODATEK


def _wstep(a: dict | None, b: dict | None) -> tuple[str, str, int] | None:
    """Werdykt, zanim dojdzie do poziomów poniżej zabiegu (jak w v13); None = porównuj poziomy."""
    if not a or not b:
        return "niepelne", "brak destylacji", 0
    pa, pb = pozycja(a), pozycja(b)
    if pa not in POROWNYWANE or pb not in POROWNYWANE:
        return "rozne", f"nie porównujemy: {pa if pa not in POROWNYWANE else pb}", 0
    wa, wb = a.get("wezel"), b.get("wezel")
    poziom = 1 if wa and wb and wa[0] == wb[0] else 0
    if pa != pb:
        return "powiazane", "pakiet i pojedyncza wizyta", poziom
    # zestaw po punkcie neutralnym tak/nie (docs TypeSafe: 0,5 = tyle samo za tak i za nie); sprawdzian 2: 0,94
    # kontra 0,34 przechodziło jako „nie jednostronne” i „włosy + broda” wychodziło tym samym co same włosy
    if _zestaw(a) != _zestaw(b):
        return "powiazane", "zestaw", poziom
    if _jednostronne(a, b, "rozszerzenie"):
        return "powiazane", "dodatek", poziom
    if not wa or not wb or wa[1] in (OGOLNIE, INNE) or wb[1] in (OGOLNIE, INNE):
        return "niepelne", "zabieg nieustalony", poziom
    if wa[1] != wb[1] and not (_zestaw(a) and _zestaw(b)):
        return "rozne", "inny zabieg", poziom
    if _zestaw(a) and _zestaw(b) and not any(a["frazy"].values()) and not any(b["frazy"].values()):
        return "niepelne", "skład nieznany", poziom  # dwa pakiety bez słowa o tym, co zawierają („Złoto Bałtyku”)
    return None


def _frazy(a: dict, b: dict) -> tuple[str, list[FrazaP], list[FrazaP]]:
    """→ (zabieg jako kontekst pytań, frazy a, frazy b). Oba zestawy: skład = zabieg główny + dodatkowe zabiegi."""
    oba_zestawy = _zestaw(a) and _zestaw(b)
    z = " / ".join(sorted({a["etykieta"], b["etykieta"]}))

    def lista(x: dict) -> list[FrazaP]:
        # skład = zbiór zabiegów ZESTAWU (model Alexa); poza zestawem zostaje tylko słowo, które według TypeSafe
        # zmienia zabieg („Remover brwi”), a nie jego cel („Lip Flip powiększenie ust”) — patrz potwierdz
        f = [(p, r, s, zr) for p in POZIOMY for r, s, zr in x["frazy"][p]]
        if oba_zestawy and all(s != x["etykieta"] for _p, _r, s, _z in f):
            f.append(("skład", "zabieg", x["etykieta"], "nazwa"))
        return f
    return z, lista(a), lista(b)


def _pozostale(fa: list[FrazaP], fb: list[FrazaP]) -> tuple[list[FrazaP], list[FrazaP]]:
    """Identyczna fraza po obu stronach = zgodna, niezależnie od roli (role bywają niezgodne: „1:1” metoda/wielkość)."""
    sa, sb = {e[2] for e in fa}, {e[2] for e in fb}
    return [e for e in fa if e[2] not in sb], [e for e in fb if e[2] not in sa]


def potrzebne_relacje(a: dict | None, b: dict | None) -> set[tuple[str, str, str, str]]:
    """Runda 1: pary różnych fraz TEJ SAMEJ roli — o nie pytamy TypeSafe, czy znaczą to samo."""
    if _wstep(a, b) is not None:
        return set()
    z, fa, fb = _frazy(a, b)
    ra, rb = _pozostale(fa, fb)
    return ({klucz_relacji(z, r, x, y) for _p, r, x, _z in ra for _q, r2, y, _z2 in fb if r2 == r and y != x}
            | {klucz_relacji(z, r, x, y) for _p, r, x, _z in fa for _q, r2, y, _z2 in rb if r2 == r and y != x})


def _rozbior(a: dict, b: dict, rel: dict) -> tuple[list[str], list[tuple[KluczCechy, bool]]]:
    """→ (poziomy ze znaną różnicą, frazy bez odpowiednika: (klucz pytania o DRUGĄ usługę, czy druga podaje tę cechę))."""
    z, fa, fb = _frazy(a, b)
    ra, rb = _pozostale(fa, fb)
    roznice: list[str] = []
    bez_odpowiednika: list[tuple[KluczCechy, bool]] = []
    for (p, r, x, zr), druga, uid in [(e, fb, b["id"]) for e in ra] + [(e, fa, a["id"]) for e in rb]:
        ta_rola = [y for _q, r2, y, _z in druga if r2 == r and y != x]
        if any(rel.get(klucz_relacji(z, r, x, y)) is True for y in ta_rola):
            continue
        if p in DOMNIEMANE:
            bez_odpowiednika.append(((uid, x, p, WARIANT if zr == "wariant" else CECHA), bool(ta_rola)))
        else:
            roznice.append(p)  # dodatek albo dodatkowy zabieg tylko w jednej = podobna
    return roznice, bez_odpowiednika


def potrzebne_domniemania(a: dict | None, b: dict | None, rel: dict) -> set[KluczCechy]:
    """Runda 2: pytania o drugą usługę dla fraz bez odpowiednika."""
    if _wstep(a, b) is not None:
        return set()
    return {k for k, _podaje in _rozbior(a, b, rel)[1]}


def porownaj_v14(a: dict | None, b: dict | None, rel: dict, dom: dict) -> tuple[str, str, int]:
    """→ (werdykt, powód, poziom pokrycia 0–5). tozsame | powiazane | niepelne | rozne.
    a, b: profile po potwierdz; rel: klucz_relacji → True (to samo); dom: KluczCechy → True (druga ma cechę / wariant)."""
    wstep = _wstep(a, b)
    if wstep is not None:
        return wstep
    roznice, bez_odpowiednika = _rozbior(a, b, rel)
    brak: list[str] = []
    for klucz, druga_podaje in bez_odpowiednika:
        if dom.get(klucz) is True:
            continue
        # druga usługa podaje tę cechę inaczej (inny obszar, inna metoda) → znana różnica; nie podaje wcale → za mało danych
        (roznice if druga_podaje else brak).append(klucz[2])
    if roznice:
        p = min(roznice, key=POZIOMY.index)
        return "powiazane", f"inny poziom: {p}", NR[p]
    if brak:
        p = min(brak, key=POZIOMY.index)
        return "niepelne", f"{p}: podaje tylko jedna", NR[p]
    return "tozsame", "zgodne wszystkie poziomy", 5


__all__: list[Any] = ["POZIOMY", "CECHA", "WARIANT", "profil_v14", "potrzebne_potwierdzenia", "potwierdz", "porownaj_v14",
                      "potrzebne_relacje", "potrzebne_domniemania", "klucz_relacji", "stan_relacji", "pytanie_tozsamosci",
                      "pytanie_cechy", "pytanie_wariantu", "pytanie_zmiany", "pytanie_dla"]
