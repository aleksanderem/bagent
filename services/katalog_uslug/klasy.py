"""Katalog usług — rozstrzygnięcie klasy różnicy jednostronnej RAZ dla rynku (hybryda z planu 29.09).

Klasa = (rdzeń zabiegu, poziom, dopisek), np. („rzęs uzupełnian”, „rdzen”, „uv”). Pytanie TypeSafe Score (3 poziomy,
routing najbliższym poziomem jak cookbook entity_alignment) na pełnym kontekście JEDNEJ oferty, która dopisek ma —
pytanie o dwie frazy bez opisu usługi nie rozstrzyga znaczenia (pomiar 28.09). Nieistotna jest tylko klasa
„nie zmienia”; „nie wiadomo” i brak odpowiedzi = istotna (zła para gorsza niż brak porównania, Alex 29.09).
"""
from __future__ import annotations

from typesafe_sdk import Score

from services.typesafe_drzewo.drzewo_v14 import _poziomy, poziom_score

NIE_ZMIENIA, NIE_WIADOMO, ZMIENIA = 0, 1, 2
OPIS_POZIOMU = {"rdzen": "metoda, technika albo rodzaj zabiegu", "gdzie_ile": "obszar, rozmiar, długość, liczba albo dla kogo",
                "etap": "etap w cyklu zabiegu", "sesje": "liczba wizyt w pakiecie", "sklad": "dodatkowa część usługi",
                "wylaczenie": "wyłączenie z usługi", "poziom": "poziom usługi", "inne": "szczegół usługi"}


def pytanie_klasy(poziom: str, dopisek: str, zabieg: str, druga: str) -> Score:
    """dopisek: słowa z oferty w oryginalnym brzmieniu; zabieg: nazwa zabiegu obu ofert; druga: nazwa oferty bez dopisku."""
    return Score(
        instructions=(f"Usługa `usluga` ma dopisek „{dopisek}” ({OPIS_POZIOMU[poziom]}). Inna oferta tego samego zabiegu "
                      f"(„{zabieg}”) — „{druga}” — tego dopisku nie ma. Czy dopisek zmienia usługę tak, że cen obu ofert "
                      "nie da się uczciwie porównać? Rozstrzygają nazwa, kategoria, opis, warianty, zabieg wybrany "
                      "w Booksy i salon `salon`."),
        criteria=_poziomy(
            ("nie zmienia: dopisek opisuje zwykłą wersję tego zabiegu albo szczegół, który nie zmienia zakresu ani ceny",
             ["„do 15 cm” przy strzyżeniu brody", "„1 osoba” przy masażu"]),
            ("nie wiadomo: z usługi i salonu nie da się tego rozstrzygnąć",
             ["dopisek to nazwa własna, której znaczenia nic w usłudze nie wyjaśnia"]),
            ("zmienia: dopisek oznacza inną metodę, inny obszar albo zakres, inną liczbę, inny etap albo dodatkową część usługi",
             ["„UV” przy uzupełnianiu rzęs", "„+ mycie” przy strzyżeniu", "„łydki” przy depilacji"])))


def rozstrzygnij(score: float | None) -> int:
    return poziom_score(score)


__all__ = ["NIE_WIADOMO", "NIE_ZMIENIA", "OPIS_POZIOMU", "ZMIENIA", "pytanie_klasy", "rozstrzygnij"]
