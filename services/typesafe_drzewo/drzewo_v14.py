"""Drzewo v14 — drzewo do poziomu zabiegu, niżej frazy obu usług porównane wprost (bd BEAUTY_AUDIT-asrk, 28.09).

Zgoda Alexa (28.09, warunkowa): cookbook TypeSafe hierarchical_classification obowiązuje do poziomu ZABIEGU
(dziedzina → zabieg; zabieg z kilkoma rodzicami jak MeSH). Poniżej — metoda, gdzie i ile, etap, dodatki, skład
zestawu — nie wybieramy z listy opcji: lista zawsze była niepełna (ocena 181 par: lasery różnych typów wychodziły
„tą samą”, identyczne pedicure „za mało danych”). Porównujemy frazy, które TypeSafe przypisał słowom każdej usługi
w jej pełnym kontekście (rola słowa) — tak, jak porównuje je człowiek. Trzy rundy pytań:
  0. frazy z kontekstu (zabieg z Booksy, kategoria cennika) uzupełniają tylko poziom, którego usługa sama nie podaje,
     i liczą się dopiero, gdy TypeSafe potwierdzi je na tej usłudze („STYLIZACJA RZĘS I BRWI” nie daje regulacji
     brwi obszaru „rzęs”); fraza zabiegu, która nazywa ten sam zabieg co węzeł, nie jest składem;
  1. identyczna fraza (dowolny poziom, bo role bywają niezgodne: „1:1” raz metoda, raz wielkość) = zgodna;
  2. cecha (rola), w której frazy się różnią → TypeSafe na pełnym kontekście DRUGIEJ usługi: czy u niej to właśnie
     frazy tej roli pierwszej, wszystkie razem (Score z trzema poziomami jak w cookbooku entity_alignment: kontekst
     przeczy — inna, część albo więcej → znana różnica, milczy → za mało danych, potwierdza → zgodne). Obie podają
     cechę → pytanie do strony, której frazy nie mają odpowiednika (Wariant.obie_strony — zawsze w obie strony)
     („oczy” kontra „okolice oczu” w mezoterapii rozstrzyga opis usługi, nie same słowa); podaje jedna → pytanie
     do drugiej („męskie” w barberze = tak). Fraza z wariantu, gdy druga cechy nie podaje — czy druga obejmuje też
     ten wariant (rodzina długości włosów kontra „cena zależy od długości”). Dodatek albo dodatkowy zabieg tylko
     w jednej = podobna (Alex 25.09).
Sprawdzian 4 (28.09, v14e → v14f): dawniej Noul „dwie frazy znaczą to samo” (za ścisły), a po nim „czy druga ma
tę cechę, nawet jeśli nie pisze” — także gdy druga podawała ją inaczej, unieważniając pierwszą odpowiedź („nogi” ma
„uda” 0,84). Na ocenionych parach zbiorów 3–4: „ta sama” bez drugiego pytania trafne 59/62, z nim 68/99. Próba
28.09: Score o samych dwóch frazach dawał „oczy” / „okolice oczu” 0,4 — bez opisu usługi nie da się tego rozstrzygnąć;
Score na pełnym kontekście dzieli prawdopodobieństwo między „nie” i „tak”, gdy fraza drugiej nie przesądza zakresu
(„maszynką” — nie wiadomo, czy samą: 0,37 / 0,63; „samą maszynką” — 1,0 „inna”); średnia wypada wtedy przy „milczy”
= za mało danych. Choice (wybór najbardziej prawdopodobnej opcji) dawał w takich parach „tak”: na zbiorze 4 „ta sama”
313, trafne 40/46, przy Score 199, trafne 28/29 — błędna „ta sama” psuje porównanie cen bardziej niż brak pary. Werdykt: wszystko zgodne → ta sama; znana różnica → podobna; brak danych → za mało danych;
inny zabieg → inna. Kod porównuje tylko identyczne frazy; resztę rozstrzyga TypeSafe. Brak odpowiedzi
TypeSafe nigdy nie daje „ta sama”.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from typesafe_sdk import Noul, NoulCriteria, Score

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
CECHA, WARIANT, ZMIANA, DOKLADNIE = "cecha", "wariant", "zmiana", "dokladnie"
INNY, MOZE, TEN_SAM = 0, 1, 2  # poziomy Score „czy właśnie taka”: przeczy / milczy / potwierdza (cookbook entity_alignment)


@dataclass(frozen=True)
class Wariant:
    """Sposób pytania „czy właśnie taka” — do pomiaru obok siebie na tych samych parach.
    razem: komplet fraz jednej roli jako „„a” + „b” — wszystko razem” (zbiór 3, 28.09: lista po przecinku czytana
    jak „którakolwiek” — rodzina obszarów wosku kontra sam „wąsik” 1,97);
    obie_strony: gdy obie usługi podają cechę, pytanie do KAŻDEJ strony, także bez nadwyżki (zbiór 3: pytana tylko
    strona bez nadwyżki — „żelem” potwierdzało „żelem + frencz” 1,93).
    synonimy: najpierw pytanie o dwie frazy tej samej roli (pytanie_tozsamosci — odmiana, synonim, skrót); „to samo”
    = zgodne na tej cesze, reszta idzie do „czy właśnie taka” (zbiór 5, 28.09: pełny kontekst wahał się przy
    „grzywka” / „grzywki” 0,84–0,92, „2:1” / „2d” 1,48, „botox” / „botoks”).
    zrodlo: do pytania „czy właśnie taka” dochodzi nazwa drugiej usługi, z której pochodzą frazy — TypeSafe sam czyta
    „+” (komplet) i „/” (alternatywa); frazy z wariantów oznaczone jako warianty do wyboru, które ta usługa musi
    objąć wszystkie (zbiory 3–5, 28.09: lista po przecinku gubi spójnik — „odcisku / modzelu” to alternatywa,
    „twarzy +szyi + dekoltu” komplet; „wszystko razem” naprawiało jedno i psuło drugie).
    Domyślnie W2 = v14f ze sprawdzianu 5. w4 (oba naraz + „ani mniej, ani więcej”) odrzucone jako za ścisłe."""
    razem: bool = False
    obie_strony: bool = False
    synonimy: bool = False
    zrodlo: bool = False


W2 = Wariant()
PRZYIMKI = frozenset("do od z ze w we na dla po u bez przy pod nad za przez wraz".split())
CUDZYSLOWY = str.maketrans("", "", "„”“\"'«»")
OPIS_POZIOMU = {"metoda": "metoda", "gdzie i ile": "obszar, wielkość albo dla kogo", "etap": "etap"}
OPIS_ROLI = {"metoda": "metoda, technika, urządzenie albo preparat", "obszar": "obszar albo część ciała, której dotyczy zabieg",
             "wielkosc": "długość, rozmiar, gęstość albo liczba", "dla_kogo": "dla kogo", "etap": "etap w cyklu zabiegu",
             "skladnik": "dodatek w cenie", "zabieg": "zabieg"}
_STAN_USLUGI = "Rozstrzygają nazwa, kategoria, opis, warianty, zabieg wybrany w Booksy i salon `salon`."

Fraza = tuple[str, str, str]           # (rola, fraza, źródło)
FrazaP = tuple[str, str, str, str]     # (poziom, rola, fraza, źródło)
KluczCechy = tuple[int, str, str, str]  # (id usługi, fraza, poziom, CECHA | WARIANT); DOKLADNIE: (id, frazy, rola, DOKLADNIE)


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


def _poziomy(*poziomy: tuple[str, list[str]]) -> list[dict[str, Any]]:
    """Poziomy Score jako obiekty z tymi samymi polami (docs TypeSafe, Score → Structured level descriptions: gdy model
    ląduje między sąsiednimi poziomami na jasnych przypadkach — próba 28.09 z samymi opisami: „1:1” / „metodą 1:1” 1,26)."""
    return [{"co": co, "przyklady": przyklady} for co, przyklady in poziomy]


ZRODLO = " ⟨"  # w kluczu: lista fraz, potem opis źródła (Wariant.zrodlo)


def pytanie_dokladnie(rola: str, frazy: str) -> Score:
    """Cecha, w której frazy usług się różnią — wszystkie frazy tej roli drugiej usługi naraz („maszynka, nożyczki”
    to co innego niż sama „maszynką”); usługa może podawać tę cechę inaczej albo wcale. Sprawdzian 4 (28.09): Noul
    „ma cechę, nawet jeśli nie pisze” mówił tak dla szczegółu, który zawęża usługę — „Strzyżenie męskie” (nożyczki,
    maszynka, trymer) „ma” „maszynką” 0,95, lipoliza „w wybrane miejsce” „ma” „uda” 0,69. Pytamy, czy usługa jest
    DOKŁADNIE taka. Komplet połączony „+” (Wariant.razem) dostaje dopisek „wszystko razem”."""
    frazy, _, zrodlo = frazy.partition(ZRODLO)
    razem = " — wszystko razem" if " + " in frazy else ""
    zrodlo = f" ({zrodlo.rstrip('⟩')})" if zrodlo else ""
    return Score(instructions=f"Inna usługa tego samego zabiegu podaje {OPIS_ROLI[rola]}: {frazy}{razem}{zrodlo}. Czy w usłudze `usluga` "
                              f"jest to właśnie {frazy}? {_STAN_USLUGI}",
                 criteria=_poziomy(
                     (f"nie: ta usługa ma inną wartość albo obejmuje więcej niż {frazy} — także inną główną metodę, "
                      "inne obszary albo kilka wariantów do wyboru",
                      ["druga podaje „łydki”, a ta usługa to depilacja całych nóg",
                       "druga podaje „hybrydowy”, a to manicure klasyczny",
                       "druga podaje jeden obszar, a tu obszar wybiera się z listy"]),
                     ("nie wiadomo: ani usługa, ani salon nic o tym nie mówią",
                      ["druga podaje „1:1”, a ta usługa nie mówi, jaką metodą"]),
                     (f"tak: z nazwy, kategorii, opisu, wariantów albo salonu wynika, że to właśnie {frazy}; drobne narzędzia "
                      "albo kroki wymienione w opisie, np. do wykończenia, tego nie zmieniają",
                      ["druga podaje „męskie”, a salon to barber",
                       "druga podaje „frezarką”, a opis tej usługi wymienia frezarkę i pilnik"])))


def poziom_score(score: float | None) -> int:
    """Najbliższy poziom Score, jak route() w cookbooku entity_alignment — punkty cięcia 0,5 i 1,5 wynikają z brzmienia
    poziomów, nie z dopasowania do danych. Brak odpowiedzi = „milczy” (nigdy „ta sama”)."""
    return MOZE if score is None else min(int(score + 0.5), TEN_SAM)


def klucz_dokladnie(uid: int, rola: str, frazy: list[str], w: Wariant = W2, zrodlo: str = "") -> KluczCechy:
    lista = (" + " if w.razem else ", ").join(f"„{f}”" for f in sorted(set(frazy)))
    return uid, lista + (f"{ZRODLO}{zrodlo}⟩" if w.zrodlo and zrodlo else ""), rola, DOKLADNIE


def _zrodlo(x: dict, frazy: list[tuple[str, str]]) -> str:
    """Wariant.zrodlo: skąd pochodzą frazy drugiej usługi — nazwa (ze spójnikami) albo jej warianty do wyboru."""
    czesci = []
    if any(zr != "wariant" for _f, zr in frazy) and x.get("nazwa"):
        czesci.append(f"w nazwie tamtej usługi: „{x['nazwa']}”")
    if any(zr == "wariant" for _f, zr in frazy):
        czesci.append("warianty: to opcje do wyboru w tamtej usłudze — ta usługa musi obejmować każdą")
    return "; ".join(czesci)


def pytanie_dla(klucz: KluczCechy) -> Noul | Score:
    _uid, fraza, pole, typ = klucz
    if typ == ZMIANA:
        return pytanie_zmiany(fraza, pole)  # dla ZMIANA trzecie pole to etykieta węzła
    if typ == DOKLADNIE:
        return pytanie_dokladnie(pole, fraza)  # dla DOKLADNIE: lista fraz i rola
    return pytanie_wariantu(fraza, pole) if typ == WARIANT else pytanie_cechy(fraza, pole)


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


def profil_v14(uid: int, frazy: list[Fraza], rek: dict | None, drzewo: dict, nazwa: str = "") -> dict | None:
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
    return {"id": uid, "nazwa": nazwa, "pozycja": rek.get("pozycja"), "zestaw": rek.get("zestaw"), "rozszerzenie": rek.get("rozszerzenie"),
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


def _sprawy(a: dict, b: dict, w: Wariant = W2, rel: dict | None = None) -> list[tuple[str, str, Any]]:
    """Role, w których frazy się różnią (po odjęciu identycznych) → (poziom, rodzaj, klucze):
    „zakres” — obie podają tę cechę: pytanie do strony o komplet fraz tej roli drugiej — tam, gdzie druga ma frazy
    bez odpowiednika, a przy Wariant.obie_strony zawsze w obie strony;
    „dokladnie” — podaje jedna: pytanie do drugiej o komplet fraz z nazwy i kontekstu;
    „wariant” — podaje jedna, fraza z wariantu: czy druga obejmuje wariant;
    „roznica” — dodatek albo dodatkowy zabieg tylko w jednej = podobna (Alex 25.09)."""
    z, fa, fb = _frazy(a, b)
    ra, rb = _pozostale(fa, fb)
    strony = ((a["id"], fa, ra), (b["id"], fb, rb))
    rel = rel or {}
    sprawy: list[tuple[str, str, Any]] = []
    for p, r in sorted({(e[0], e[1]) for e in ra + rb}, key=lambda pr: (POZIOMY.index(pr[0]), pr[1])):
        wszystkie = {uid: [x for q, r2, x, _zr in f if (q, r2) == (p, r)] for uid, f, _reszta in strony}
        zrodla = {uid: [(x, zr) for q, r2, x, zr in f if (q, r2) == (p, r)] for uid, f, _reszta in strony}
        profile = {a["id"]: a, b["id"]: b}
        zostalo = {uid: [(x, zr) for q, r2, x, zr in reszta if (q, r2) == (p, r)] for uid, _f, reszta in strony}
        (ua, _fa, _ra), (ub, _fb, _rb) = strony
        druga = {ua: ub, ub: ua}
        if all(wszystkie.values()):
            if w.synonimy:  # fraza, którą TypeSafe uznał za tę samą co któraś fraza drugiej, nie jest nadwyżką
                zostalo = {u: [(x, zr) for x, zr in zostalo[u]
                               if not any(rel.get(klucz_relacji(z, r, x, y)) is True for y in wszystkie[druga[u]] if y != x)]
                           for u in (ua, ub)}
                if not any(zostalo.values()):
                    continue
            sprawy.append((p, "zakres", [klucz_dokladnie(druga[u], r, wszystkie[u], w, _zrodlo(profile[u], zrodla[u]))
                                         for u in (ua, ub) if w.obie_strony or zostalo[u]]))
            continue
        u = ua if wszystkie[ua] else ub
        if p not in DOMNIEMANE:
            sprawy.append((p, "roznica", None))
            continue
        sprawy += [(p, "wariant", (druga[u], x, p, WARIANT)) for x, zr in zostalo[u] if zr == "wariant"]
        nazwy = [x for x, zr in zostalo[u] if zr != "wariant"]
        if nazwy:
            sprawy.append((p, "dokladnie", [klucz_dokladnie(druga[u], r, nazwy, w,
                                                            _zrodlo(profile[u], [(x, zr) for x, zr in zostalo[u] if zr != "wariant"]))]))
    return sprawy


def potrzebne_synonimy(a: dict | None, b: dict | None, w: Wariant = W2) -> set[tuple[str, str, str, str]]:
    """Wariant.synonimy, runda przed „czy właśnie taka”: pary różnych fraz tej samej roli, gdy obie usługi ją podają."""
    if not w.synonimy or _wstep(a, b) is not None:
        return set()
    z, fa, fb = _frazy(a, b)
    ra, rb = _pozostale(fa, fb)
    return ({klucz_relacji(z, r, x, y) for _p, r, x, _z in ra for _q, r2, y, _z2 in fb if r2 == r and y != x}
            | {klucz_relacji(z, r, x, y) for _p, r, x, _z in rb for _q, r2, y, _z2 in fa if r2 == r and y != x})


def potrzebne_domniemania(a: dict | None, b: dict | None, w: Wariant = W2, rel: dict | None = None) -> set[KluczCechy]:
    """Pytania o cechy, w których frazy się różnią — każde o jedną usługę na jej pełnym kontekście."""
    if _wstep(a, b) is not None:
        return set()
    return {k for _p, rodzaj, d in _sprawy(a, b, w, rel) if rodzaj != "roznica" for k in ([d] if rodzaj == "wariant" else d)}


def porownaj_v14(a: dict | None, b: dict | None, poz: dict, dom: dict, w: Wariant = W2,
                 rel: dict | None = None) -> tuple[str, str, int]:
    """→ (werdykt, powód, poziom pokrycia 0–5). tozsame | powiazane | niepelne | rozne.
    a, b: profile po potwierdz; poz: klucz „czy właśnie taka” → INNY | MOZE | TEN_SAM (poziom_score odpowiedzi);
    dom: klucz wariantu → True (druga obejmuje wariant)."""
    wstep = _wstep(a, b)
    if wstep is not None:
        return wstep
    roznice: list[str] = []
    brak: list[tuple[str, str]] = []
    for p, rodzaj, k in _sprawy(a, b, w, rel):
        if rodzaj == "roznica":
            roznice.append(p)
            continue
        if rodzaj == "wariant":
            if dom.get(k) is not True:
                brak.append((p, "podaje tylko jedna"))
            continue
        lv = min(poz.get(x, MOZE) for x in k)  # obie strony muszą potwierdzić
        if lv == TEN_SAM:
            continue
        if lv == INNY or p not in DOMNIEMANE:  # skład i dodatek: zgadza tylko „ten sam zakres”
            roznice.append(p)
        else:
            brak.append((p, "nie wiadomo, czy to samo" if rodzaj == "zakres" else "podaje tylko jedna"))
    if roznice:
        p = min(roznice, key=POZIOMY.index)
        return "powiazane", f"inny poziom: {p}", NR[p]
    if brak:
        p, powod = min(brak, key=lambda e: POZIOMY.index(e[0]))
        return "niepelne", f"{p}: {powod}", NR[p]
    return "tozsame", "zgodne wszystkie poziomy", 5


__all__: list[Any] = ["POZIOMY", "CECHA", "WARIANT", "ZMIANA", "DOKLADNIE", "INNY", "MOZE", "TEN_SAM", "Wariant", "W2", "profil_v14",
                      "potrzebne_potwierdzenia", "potwierdz", "porownaj_v14", "potrzebne_domniemania", "potrzebne_synonimy",
                      "klucz_relacji", "klucz_dokladnie", "stan_relacji", "poziom_score", "pytanie_tozsamosci", "pytanie_cechy",
                      "pytanie_wariantu", "pytanie_zmiany", "pytanie_dokladnie", "pytanie_dla"]
