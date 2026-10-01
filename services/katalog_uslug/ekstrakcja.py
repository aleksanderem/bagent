"""Katalog usług — wyciąganie cech oferty (plan zatwierdzony 29.09).

Model (GLM z abonamentu Z.ai) rozkłada ofertę na frazy skopiowane z jej pól. Kod sprawdza odpowiedź:
liczba rekordów (zła = odrzut całej paczki), id, dosłowna nazwa, frazy wyłącznie z tekstu własnej oferty
(przeciek od sąsiada w paczce = odrzut rekordu), pokrycie słów nazwy i wariantu — słowo nieprzypisane do
zabiegu, cechy ani szumu trafia do `nieprzypisane`, a taka oferta nie może dostać „ta sama”.
Czy cecha jest wyróżniająca, rozstrzyga słownik raz na (zabieg, wartość) — nie ten krok.
"""
from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any

from services.katalog_uslug.normalizacja import normalizuj

POZYCJE = ("zabieg", "pakiet", "zestaw", "dodatek", "produkt", "konsultacja", "voucher", "szkolenie", "inne")
ROLE = ("metoda", "obszar", "rozmiar", "liczba", "dla_kogo", "miejsce", "etap", "sesje", "sklad", "wylaczenie", "poziom",
        "specjalista", "inne")
POLA = ("nazwa", "wariant", "kategoria", "opis", "zabieg_booksy")
OPIS_ZNAKOW = 300
WERSJA_PROMPTU = 2  # zmiana treści PROMPT = nowa wersja (osobna pamięć wyników)
LACZNIKI = frozenset({"i", "z", "ze", "na", "do", "w", "we", "oraz", "lub", "albo", "dla", "+", "/", "od", "po", "a"})

PROMPT = """Rozkładasz oferty z cenników salonów (Booksy) na części. Dla KAŻDEJ oferty z listy zwróć jeden rekord.

Zasady:
1. Frazy KOPIUJESZ dosłownie z pól oferty (nazwa, wariant, kategoria, opis, zabieg_booksy) — te same słowa
   w tej samej formie i z tymi samymi końcówkami, bez tłumaczenia, poprawiania literówek i dopisywania.
   Nie przenosisz słów między ofertami.
2. Każde słowo pól „nazwa” i „wariant” trafia do jednej frazy: zabiegu, cechy albo szumu. Słowa „combo”,
   „komplet”, „zestaw”, „pakiet”, „seria” nigdy nie są szumem.
3. zabieg = najkrótsza fraza nazywająca czynność (np. „Strzyżenie”, „Masaż”, „Depilacja”, „Manicure”,
   „Mezoterapia”); przymiotniki i dopełnienia mówiące JAK, CZYM albo JAKI rodzaj idą do cechy „metoda”.
   Kategoria i zabieg_booksy uzupełniają TYLKO to, czego nazwa i wariant nie mówią: zabieg (np. nazwa „Pachy”
   w kategorii „Depilacja laserowa”), metodę i dla kogo („dla mężczyzn”). Czego nazwa już mówi, nie dublujesz.
4. Role cech: metoda (technika, rodzaj, urządzenie, preparat, marka), obszar (część ciała, miejsce),
   rozmiar (długość, rozmiar, gęstość, objętość, np. „długie”, „do ramion”, „2:1”, „4-6D”),
   liczba (sztuki, obszary, osoby, „każdy kolejny”), dla_kogo (męskie, damskie, dziecięce, dla par),
   etap (pierwszy zabieg, uzupełnienie, korekta, zdjęcie, kontrola, „do 3 tyg.”), sesje (pakiet / seria N),
   sklad (każdy dodatkowy element poza zabiegiem głównym: „+ mycie”, „z maską”, „+ ampułka”),
   wylaczenie („bez strzyżenia”, „bez malowania”), poziom (basic, premium, lux, express, mini, rozszerzony),
   specjalista (poziom lub imię wykonawcy: top stylist, master, junior, imię), inne (coś ważnego spoza listy).
5. szum = słowa bez znaczenia dla usługi: promocje, rabaty, czas trwania, emotki, zachęty.
6. Z opisu bierzesz WYŁĄCZNIE: wyłączenia („bez strzyżenia”), osobno płatne usługi wliczone w cenę („w cenie
   strzyżenie”) i liczby („cena za 1 modzel”, „każdy kolejny”). Zwykłych kroków usługi i zachęt z opisu nie wypisujesz.
7. pozycja: zabieg (zwykła pojedyncza usługa — domyślnie) | pakiet (kilka wizyt tego samego) | zestaw (kilka różnych
   zabiegów razem) | dodatek | produkt | konsultacja | voucher | inne (tylko gdy żadne z powyższych).

Odpowiedź — wyłącznie JSON:
{"oferty": [{"id": "<id oferty>", "nazwa": "<dosłowna nazwa oferty>", "pozycja": "...",
  "zabieg": {"fraza": "...", "zrodlo": "nazwa|wariant|kategoria|opis|zabieg_booksy"},
  "cechy": [{"rola": "...", "fraza": "...", "zrodlo": "..."}], "szum": ["..."]}]}

Oferty:
"""


@dataclass(frozen=True)
class Oferta:
    id: str
    typ_salonu: str
    kategoria: str
    nazwa: str
    wariant: str
    zabieg_booksy: str
    opis: str
    cena_zl: float | None


def oferty_z_uslugi(u: dict[str, Any]) -> list[Oferta]:
    """Usługa z ≥ 2 wariantami z etykietą → oferta na wariant (decyzja Alexa 29.09); inaczej jedna oferta."""
    war = [w for w in u.get("warianty") or [] if (w.get("label") or "").strip()]
    wspolne = {"typ_salonu": u.get("typ_salonu") or "", "kategoria": u.get("kategoria") or "",
               "nazwa": u.get("nazwa") or "", "zabieg_booksy": u.get("zabieg_booksy") or "",
               "opis": (u.get("opis") or "")[:OPIS_ZNAKOW]}
    if len(war) >= 2:
        return [Oferta(id=f"{u['id']}#{i}", wariant=w["label"].strip(), cena_zl=w.get("cena_zl"), **wspolne)
                for i, w in enumerate(war)]
    pierwszy = (u.get("warianty") or [{}])[0]
    cena = pierwszy.get("cena_zl") if pierwszy.get("cena_zl") is not None else (u.get("cena_gr") or 0) / 100
    return [Oferta(id=str(u["id"]), wariant=war[0]["label"].strip() if war else "", cena_zl=cena, **wspolne)]


def prompt(oferty: list[Oferta]) -> str:
    dane = [{k: v for k, v in asdict(o).items() if k != "cena_zl" and (v or k == "id")} for o in oferty]
    return PROMPT + json.dumps(dane, ensure_ascii=False, indent=1)


# Rozbiór DZIAŁU (decyzja Alexa 1.10, po raporcie 279): zamiast reguł w kodzie, które słowa nagłówka działu i nazwy
# salonu dokleić do oferty (każdy nowy układ cennika wymagał nowej reguły), model czyta dział w całości — jak klientka
# czytająca cennik — i sam przypisuje każdej ofercie to, co jej dotyczy. Jedno wywołanie widzi wszystkie oferty działu,
# więc decyzja o nagłówku jest jedna dla działu, nie losowa przy każdej ofercie (niespójność z pomiaru 29.09).
WERSJA_DZIALU = 3
ZRODLA = ("nazwa", "wariant", "zabieg_booksy", "opis", "dzial", "salon")

PROMPT_DZIALU = """Rozkładasz oferty z cenników salonów (Booksy) na części. Oferty są pogrupowane w DZIAŁY: dział to
nagłówek, pod którym salon wypisał usługi, z nazwą i typem salonu. Czytasz dział w całości, jak klientka czytająca
cennik: nagłówek działu i nazwa salonu mówią coś o usługach pod nimi. Dla KAŻDEJ oferty zwróć jeden rekord.

Zasady:
1. Frazy KOPIUJESZ dosłownie — te same słowa w tej samej formie i z tymi samymi końcówkami, bez tłumaczenia,
   poprawiania literówek i dopisywania. Źródło frazy: nazwa | wariant | zabieg_booksy | opis oferty albo dzial
   (nagłówek jej działu) | salon (nazwa salonu). Nie przenosisz słów z innego działu ani z innej oferty.
2. Każde słowo pól „nazwa” i „wariant” trafia do jednej frazy: zabiegu, cechy albo szumu. Słowa „combo”,
   „komplet”, „zestaw”, „pakiet”, „seria” nigdy nie są szumem.
3. Rekord opisuje usługę w całości. Nagłówek działu, nazwa salonu i zabieg_booksy uzupełniają to, czego nazwa
   i wariant oferty nie mówią: zabieg, metodę, obszar, dla kogo, liczbę zabiegów w pakiecie. Gdy nagłówek wymienia
   kilka rodzajów usług, ofercie przypisujesz tylko część, która jej dotyczy; to, co nagłówek mówi o wszystkich
   usługach działu, przypisujesz każdej. Czego oferta mówi sama, nie dublujesz; gdy mówi co innego niż nagłówek,
   liczy się oferta. Nazwa salonu dotyczy usług tylko wtedy, gdy nazywa jedną rzecz, którą salon robi.
4. zabieg = najkrótsza fraza nazywająca czynność (np. „Strzyżenie”, „Masaż”, „Depilacja”, „Manicure”,
   „Mezoterapia”); przymiotniki i dopełnienia mówiące JAK, CZYM albo JAKI rodzaj idą do cechy „metoda”.
   Samo urządzenie, metoda albo część ciała nie jest czynnością — gdy nazwa oferty nie mówi, co się robi,
   zabieg pochodzi z nagłówka działu albo z zabieg_booksy, a słowo z nazwy idzie do cechy.
5. Role cech: metoda (technika, rodzaj, urządzenie, preparat, marka), obszar (część ciała, miejsce),
   rozmiar (długość, rozmiar, gęstość, objętość, np. „długie”, „do ramion”, „2:1”, „4-6D”),
   liczba (sztuki, obszary, osoby, „każdy kolejny”), dla_kogo (męskie, damskie, dziecięce, dla par),
   etap (pierwszy zabieg, uzupełnienie, korekta, zdjęcie, kontrola, „do 3 tyg.”), sesje (pakiet / seria N),
   sklad (każdy dodatkowy element poza zabiegiem głównym: „+ mycie”, „z maską”, „+ ampułka”),
   wylaczenie („bez strzyżenia”, „bez malowania”), poziom (basic, premium, lux, express, mini, rozszerzony),
   miejsce (gdzie wykonywana: „mobilne”, „z dojazdem”), specjalista (poziom lub imię wykonawcy: top stylist,
   master, junior, imię), inne (coś ważnego spoza listy).
6. szum = słowa bez znaczenia dla usługi: promocje, rabaty, czas trwania, numeracja, emotki, zachęty.
7. Z opisu bierzesz WYŁĄCZNIE: wyłączenia („bez strzyżenia”), osobno płatne usługi wliczone w cenę („w cenie
   strzyżenie”) i liczby („cena za 1 modzel”, „każdy kolejny”). Zwykłych kroków usługi i zachęt z opisu nie wypisujesz.
8. pozycja: zabieg (zwykła pojedyncza usługa — domyślnie) | pakiet (kilka wizyt tego samego) | zestaw (kilka różnych
   zabiegów razem) | dodatek | produkt | konsultacja | voucher | szkolenie | inne. Nagłówek działu też o tym mówi.

Odpowiedź — wyłącznie JSON:
{"oferty": [{"id": "<id oferty>", "nazwa": "<dosłowna nazwa oferty>", "pozycja": "...",
  "zabieg": {"fraza": "...", "zrodlo": "nazwa|wariant|zabieg_booksy|opis|dzial|salon"},
  "cechy": [{"rola": "...", "fraza": "...", "zrodlo": "..."}], "szum": ["..."]}]}

Działy:
"""


def prompt_dzialu(dzialy: list[tuple[str, str, list[Oferta]]]) -> str:
    """dzialy = [(nagłówek działu, nazwa salonu, oferty działu)] — typ salonu z ofert (ten sam w dziale)."""
    dane = [{"dzial": naglowek, "salon": salon, "typ_salonu": of[0].typ_salonu if of else "",
             "oferty": [{k: v for k, v in asdict(o).items()
                         if k in ("id", "nazwa", "wariant", "zabieg_booksy", "opis") and (v or k == "id")} for o in of]}
            for naglowek, salon, of in dzialy]
    return PROMPT_DZIALU + json.dumps(dane, ensure_ascii=False, indent=1)


def _frazy(rek: dict[str, Any]) -> list[str]:
    z = rek.get("zabieg") or {}
    return [str(z.get("fraza") or ""), *(str(c.get("fraza") or "") for c in rek.get("cechy") or []),
            *(str(s) for s in rek.get("szum") or [])]


def _w_tekscie(fraza: str, tekst: str) -> bool:
    f = normalizuj(fraza)
    return not f or f" {f} " in f" {tekst} "


def waliduj(oferty: list[Oferta], odp: dict[str, Any],
            salony: dict[str, str] | None = None) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """→ (id oferty → rekord z polem `nieprzypisane`, błędy). Zła liczba rekordów odrzuca całą paczkę.
    `salony` (rozbiór działu): id oferty → nazwa salonu — fraza z nazwy salonu też jest tekstem tej oferty."""
    rek = odp.get("oferty") if isinstance(odp, dict) else None
    if not isinstance(rek, list) or len(rek) != len(oferty):
        return {}, [f"liczba rekordów {len(rek) if isinstance(rek, list) else 'brak'} ≠ {len(oferty)} ofert"]
    po_id = {o.id: o for o in oferty}
    wynik: dict[str, dict[str, Any]] = {}
    bledy: list[str] = []
    for r in rek:
        oid = str(r.get("id"))
        o = po_id.get(oid)
        if o is None:
            bledy.append(f"id {oid}: nieznane w paczce")
            continue
        if normalizuj(str(r.get("nazwa") or "")) != normalizuj(o.nazwa):
            bledy.append(f"{oid}: nazwa w odpowiedzi ≠ nazwa oferty")
            continue
        tekst = " ".join(normalizuj(t) for t in (*(getattr(o, p) for p in POLA), (salony or {}).get(oid, "")))
        obce = [f for f in _frazy(r) if not _w_tekscie(f, tekst)]
        if obce:  # parafraza albo przeciek od sąsiada — rekord zostaje, ale nie może dać „ta sama”
            bledy.append(f"{oid}: fraza spoza tekstu oferty: {obce[:3]}")
        pokryte = {t for f in _frazy(r) if f not in obce for t in normalizuj(f).split()}
        slowa = normalizuj(f"{o.nazwa} {o.wariant}").split()
        brak = [t for i, t in enumerate(slowa) if t not in pokryte and t not in LACZNIKI and t not in slowa[:i]]
        wynik[oid] = {**r, "cechy": [{**c, "rola": c.get("rola") if c.get("rola") in ROLE else "inne"}
                                      for c in r.get("cechy") or [] if str(c.get("fraza") or "") not in obce],
                      "nieprzypisane": brak, "obce": obce, "wariant": o.wariant}
    return wynik, bledy

PROMPT_KATEGORII = """Rozkładasz NAZWY KATEGORII z cenników salonów (Booksy) — sekcje, w których salon grupuje swoje usługi.
Dla KAŻDEJ kategorii z listy zwróć jeden rekord (pole „nazwa” to nazwa kategorii).

Zasady:
1. Frazy KOPIUJESZ dosłownie z nazwy kategorii — te same słowa, formy i końcówki.
2. zabieg = czynność, jeśli kategoria ją nazywa („Depilacja laserowa”, „Masaże”, „Fale radiowe”); gdy nazwa kategorii
   to tylko część ciała, grupa klientów albo marketing — zabieg pusty ("").
3. Role cech: metoda (technika, urządzenie, marka: „laserowa”, „PRIMELASE”, „Soprano”), obszar, dla_kogo („kobiety”,
   „mężczyzn”, „dla dwojga”, „dzieci”), miejsce (gdzie wykonywana: „mobilne”, „z dojazdem”, „w domu klienta”),
   poziom (premium, lux…), specjalista (imię lub poziom wykonawcy), inne.
4. szum = numeracja („15.”), promocje i rabaty („-20%”, „PROMO”), emotki, zachęty.

Odpowiedź — wyłącznie JSON:
{"oferty": [{"id": "<id>", "nazwa": "<dosłowna nazwa kategorii>", "pozycja": "inne",
  "zabieg": {"fraza": "...", "zrodlo": "nazwa"}, "cechy": [{"rola": "...", "fraza": "...", "zrodlo": "nazwa"}], "szum": ["..."]}]}

Kategorie:
"""


def prompt_kategorii(kategorie: list[Oferta]) -> str:
    return PROMPT_KATEGORII + json.dumps([{"id": k.id, "nazwa": k.nazwa} for k in kategorie], ensure_ascii=False, indent=1)


# Co salon sprzedaje w sekcji cennika — osobne krótkie pytanie, bo nazwa oferty tego nie mówi („Japoński masaż twarzy
# KOBIDO” za 2000 zł w kategorii „SZKOLENIA” dostał „ta sama” z 13 masażami Kobido — sprawdzian 8, 30.09).
# Wersja 2 (30.09, przegląd odpowiedzi v1 na 2271 kategoriach): bez „dodatek” — sekcje „Dodatki” / „Usługi dodatkowe”
# trzymają też pełne usługi (henna rzęs, wosk nosa, modelowanie), dopłatę rozpoznaje pozycja samej oferty; wykonawca
# w nazwie („Manicure Instruktor”) to nie kurs — v1 wyrzucało tak zwykły manicure za 85 zł.
WERSJA_POZYCJI = 2
POZYCJE_KATEGORII = ("uslugi", "szkolenie", "voucher", "produkt")
PROMPT_POZYCJI_KATEGORII = """Oceniasz NAZWY KATEGORII z cenników salonów (Booksy) — sekcje, w których salon grupuje to, co sprzedaje.
Dla KAŻDEJ kategorii z listy zwróć jeden rekord: co salon sprzedaje w tej sekcji.

pozycja:
- uslugi — zabiegi i usługi wykonywane na klientce (domyślnie; także gdy kategoria to nazwa zabiegu, części ciała,
  grupa klientów, marketing albo dopłaty i usługi dodatkowe; „Kosmetyka”, „Kosmetologia”, „Pielęgnacja” to dziedziny
  zabiegów). Imię, poziom albo tytuł wykonawcy w nazwie („Instruktor”, „Master”, „Top stylist”, „Junior”) to nadal
  uslugi — mówi, KTO wykonuje zabieg, a nie że to kurs.
- szkolenie — szkolenia, kursy i warsztaty sprzedawane kursantkom oraz zabiegi na modelkach w ramach nauki
- voucher — vouchery, bony, karty podarunkowe
- produkt — produkty i kosmetyki na sprzedaż do domu (sklep)
Gdy kategoria łączy usługi z czymś z listy („Manicure i vouchery”) — uslugi.
fraza = słowa z nazwy kategorii, które to mówią, skopiowane dosłownie; przy „uslugi” pusta ("").

Odpowiedź — wyłącznie JSON:
{"kategorie": [{"id": "<id>", "nazwa": "<dosłowna nazwa kategorii>", "pozycja": "...", "fraza": "..."}]}

Kategorie:
"""


def prompt_pozycji_kategorii(kategorie: list[Oferta]) -> str:
    return PROMPT_POZYCJI_KATEGORII + json.dumps([{"id": k.id, "nazwa": k.nazwa} for k in kategorie], ensure_ascii=False,
                                                 indent=1)


def waliduj_pozycje(kategorie: list[Oferta], odp: dict[str, Any]) -> tuple[dict[str, dict[str, str]], list[str]]:
    """→ (id kategorii → {pozycja, fraza}, błędy). Pozycja inna niż usługi tylko z frazą skopiowaną z nazwy kategorii —
    bez niej zostaje „uslugi” (lepiej porównać niż wyrzucić zwykłą usługę przez zgadnięcie modelu)."""
    rek = odp.get("kategorie") if isinstance(odp, dict) else None
    if not isinstance(rek, list) or len(rek) != len(kategorie):
        return {}, [f"liczba rekordów {len(rek) if isinstance(rek, list) else 'brak'} ≠ {len(kategorie)} kategorii"]
    po_id = {k.id: k for k in kategorie}
    wynik: dict[str, dict[str, str]] = {}
    bledy: list[str] = []
    for r in rek:
        kid, poz, fraza = str(r.get("id")), str(r.get("pozycja") or ""), str(r.get("fraza") or "")
        k = po_id.get(kid)
        if k is None or normalizuj(str(r.get("nazwa") or "")) != normalizuj(k.nazwa):
            bledy.append(f"{kid}: nieznane id albo nazwa ≠ nazwa kategorii")
            continue
        if poz not in POZYCJE_KATEGORII:
            bledy.append(f"{kid}: pozycja spoza listy: {poz!r}")
            poz = "uslugi"
        elif poz != "uslugi" and not (normalizuj(fraza) and _w_tekscie(fraza, normalizuj(k.nazwa))):
            bledy.append(f"{kid}: pozycja {poz} bez frazy z nazwy kategorii: {fraza!r}")
            poz = "uslugi"
        wynik[kid] = {"pozycja": poz, "fraza": fraza if poz != "uslugi" else ""}
    return wynik, bledy


PROMPT_SALONOW = """Rozkładasz NAZWY SALONÓW z Booksy. Dla KAŻDEJ nazwy z listy zwróć jeden rekord (pole „nazwa” to nazwa salonu).

Zasady:
1. Frazy KOPIUJESZ dosłownie z nazwy — te same słowa, formy i końcówki.
2. zabieg = czynność, jeśli nazwa ją nazywa („Depilacja”, „Masaż”, „Manicure”); gdy nie nazywa — zabieg pusty ("").
3. Role cech: metoda (technika albo urządzenie, którym salon pracuje: „Laser”, „Wax”, „Sugaring”), obszar, dla_kogo
   („Men”, „Kids”), miejsce („mobilny”, „z dojazdem”), specjalista (imię, nazwisko, zawód: „Kosmetolog”, „Podolog”), inne.
4. szum = nazwa własna marki, miasto, dzielnica, słowa ogólne („Studio”, „Salon”, „Beauty”, „Instytut”, „Atelier”), emotki.

Odpowiedź — wyłącznie JSON:
{"oferty": [{"id": "<id>", "nazwa": "<dosłowna nazwa salonu>", "pozycja": "inne",
  "zabieg": {"fraza": "...", "zrodlo": "nazwa"}, "cechy": [{"rola": "...", "fraza": "...", "zrodlo": "nazwa"}], "szum": ["..."]}]}

Salony:
"""
_ZLEPEK = re.compile(r"(?<=[a-ząćęłńóśźż])(?=[A-ZĄĆĘŁŃÓŚŹŻ])")


def rozdziel_zlepki(nazwa: str) -> str:
    """„LaserPoznań” → „Laser Poznań”: nazwy salonów bywają zlepkiem słów (proste czyszczenie tekstu)."""
    return _ZLEPEK.sub(" ", nazwa)


def prompt_salonow(salony: list[Oferta]) -> str:
    return PROMPT_SALONOW + json.dumps([{"id": k.id, "nazwa": k.nazwa} for k in salony], ensure_ascii=False, indent=1)
