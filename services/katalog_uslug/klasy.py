"""Katalog usług — rozstrzygnięcie klasy różnicy jednostronnej RAZ dla rynku (hybryda z planu 29.09).

Klasa = (rdzeń zabiegu, poziom, dopisek), np. („rzęs uzupełnian”, „rdzen”, „uv”). Pytanie TypeSafe Score (3 poziomy,
routing najbliższym poziomem jak cookbook entity_alignment) na pełnym kontekście JEDNEJ oferty, która dopisek ma —
pytanie o dwie frazy bez opisu usługi nie rozstrzyga znaczenia (pomiar 28.09). Nieistotna jest tylko klasa
„nie zmienia”; „nie wiadomo” i brak odpowiedzi = istotna (zła para gorsza niż brak porównania, Alex 29.09).
"""
from __future__ import annotations

from typesafe_sdk import Choice, Score

from services.katalog_uslug.slownik import INNE, SZERSZE, TO_SAMO, WEZSZE
from services.typesafe_drzewo.drzewo_v14 import _poziomy, poziom_score

NIE_ZMIENIA, NIE_WIADOMO, ZMIENIA = 0, 1, 2
OPIS_POZIOMU = {"rdzen": "metoda, technika albo rodzaj zabiegu", "gdzie_ile": "obszar, rozmiar, długość, liczba albo dla kogo",
                "etap": "etap w cyklu zabiegu", "sesje": "liczba wizyt w pakiecie", "sklad": "dodatkowa część usługi",
                "wylaczenie": "wyłączenie z usługi", "poziom": "poziom usługi", "inne": "szczegół usługi"}


WERSJA_PYTANIA = 2  # v2 (29.09): obie oferty wprost, dopisek = tylko słowa różnicy; v1 zakładało „ten sam zabieg”


def pytanie_klasy(poziom: str, dopisek: str, oferta: str, druga: str) -> Score:
    """dopisek: słowa, którymi różnią się oferty (w oryginalnym brzmieniu); oferta: nazwa oferty z dopiskiem;
    druga: nazwa oferty bez dopisku. v1 pisało „inna oferta tego samego zabiegu” — przesądzało odpowiedź
    (raport testowy 29.09: „Laserowe usuwanie tatuażu” uznane za tę samą usługę co „Tatuaż”, 0,83)."""
    return Score(
        instructions=(f"Oferta `usluga` to „{oferta}”. Inna oferta w innym salonie to „{druga}”. Pierwsza ma dodatkowo "
                      f"słowa „{dopisek}” ({OPIS_POZIOMU[poziom]}), których druga nie ma. Czy przez te słowa to inna "
                      "usługa albo inny zakres — taki, że cen obu ofert nie da się uczciwie porównać? Rozstrzygają "
                      "nazwa, kategoria, opis, warianty, zabieg wybrany w Booksy i salon `salon`."),
        criteria=_poziomy(
            ("nie zmienia: te słowa opisują zwykłą wersję tej samej usługi albo szczegół, który nie zmienia zakresu ani ceny",
             ["„do 15 cm” przy strzyżeniu brody", "„1 osoba” przy masażu"]),
            ("nie wiadomo: z ofert i salonu nie da się tego rozstrzygnąć",
             ["słowo to nazwa własna, której znaczenia nic w ofercie nie wyjaśnia"]),
            ("zmienia: te słowa oznaczają inną usługę, inną metodę, inny obszar albo zakres, inną liczbę, inny etap "
             "albo dodatkową część usługi",
             ["„UV” przy uzupełnianiu rzęs", "„zdjęcie” przy manicure hybrydowym", "„łydki” przy depilacji"])))


WERSJA_ZAMIANY = 2  # v2: opcja „nie wiadomo” (docs TypeSafe: wyjście, gdy nic nie pasuje) — v1 bez niej wciskało nazwy
# własne w „to samo” (raport testowy 29.09: „Cyber Bites” / „Przekłucie” 0,88, „Medusa” / „Smile” 0,87)
NIE_WIADOMO_REL = "nie_wiadomo"
PROG_TO_SAMO = 0.8  # ten sam ostry próg co przy scalaniu słownika synonimów (synonimy.py) — nie strojony na parach


def pytanie_zamiany(slowa_a: str, slowa_b: str, oferta_a: str, oferta_b: str) -> Choice:
    """Różnica po obu stronach (podpis.zamiana_slow): relacja 4-stanowa jak w słowniku (plan 29.09: to samo / węższe /
    szersze / inne), ale w kontekście OBU ofert — znaczenie słowa zależy od zabiegu. Raz na klasę (wspólne słowa,
    słowa A, słowa B), z pamięcią. Oba opisy w jednym stanie jak w cookbooku entity_alignment; pytamy o parę fraz,
    nie o całą parę ofert. Zamiana nieistotna tylko przy „to samo” (zamiana_rownowazna)."""
    return Choice(
        instructions=(f"Oferta `oferta_a.usluga` to „{oferta_a}”, oferta `oferta_b.usluga` z innego salonu to „{oferta_b}”. "
                      f"Mają wspólną resztę nazwy, a różnią się słowami: pierwsza ma „{slowa_a}”, druga „{slowa_b}”. "
                      f"Jak ma się znaczenie „{slowa_a}” do „{slowa_b}” w tych dwóch ofertach? Rozstrzygają nazwa, kategoria, "
                      "opis, warianty, zabieg wybrany w Booksy i salon obu ofert."),
        criteria={
            TO_SAMO: {"what": f"„{slowa_a}” i „{slowa_b}” nazywają tu tę samą rzecz — ta sama usługa w tym samym zakresie, "
                              "tylko inaczej opisana",
                      "not_for": "inny obszar, metoda, rozmiar, liczba, etap albo dodatkowa część usługi",
                      "examples": ["„męskie” i „dla panów” przy strzyżeniu", "„twarzy” i „face” przy oczyszczaniu wodorowym"]},
            WEZSZE: {"what": f"„{slowa_a}” to część albo szczególny przypadek „{slowa_b}” — węższy zakres",
                     "not_for": "ta sama rzecz inaczej nazwana", "examples": ["„łydki” wobec „nóg” przy depilacji"]},
            SZERSZE: {"what": f"„{slowa_a}” obejmuje „{slowa_b}” i coś więcej — szerszy zakres",
                      "not_for": "ta sama rzecz inaczej nazwana", "examples": ["„całe nogi” wobec „łydek” przy depilacji"]},
            INNE: {"what": "różne rzeczy — inna usługa, metoda, obszar, rozmiar, liczba, etap albo dodatkowa część usługi",
                   "not_for": "ta sama rzecz inaczej nazwana",
                   "examples": ["„pachy” i „łydki” przy depilacji", "„klasyczny” i „hybrydowy” przy manicure"]},
            NIE_WIADOMO_REL: {"what": "z ofert i salonów nie da się rozstrzygnąć, co znaczy któreś ze słów — nazwa własna, "
                                      "marka albo określenie branżowe, którego nic w ofercie nie wyjaśnia",
                              "not_for": "słowa, których znaczenie wynika z ofert",
                              "examples": ["nazwa techniki salonu bez opisu"]}})


def zamiana_rownowazna(wpis: dict) -> bool:
    """Wpis z pamięci zamian → czy różne słowa to ta sama rzecz. Brak odpowiedzi = nie."""
    return wpis.get("relacja") == TO_SAMO and (wpis.get("rozklad") or {}).get(TO_SAMO, 0.0) >= PROG_TO_SAMO


def rozstrzygnij(score: float | None) -> int:
    return poziom_score(score)


__all__ = ["NIE_WIADOMO", "NIE_WIADOMO_REL", "NIE_ZMIENIA", "OPIS_POZIOMU", "PROG_TO_SAMO", "WERSJA_PYTANIA", "WERSJA_ZAMIANY", "ZMIENIA",
           "pytanie_klasy", "pytanie_zamiany", "rozstrzygnij", "zamiana_rownowazna"]
