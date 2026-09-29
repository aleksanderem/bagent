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


def rozstrzygnij(score: float | None) -> int:
    return poziom_score(score)


__all__ = ["NIE_WIADOMO", "NIE_ZMIENIA", "OPIS_POZIOMU", "WERSJA_PYTANIA", "ZMIENIA", "pytanie_klasy", "rozstrzygnij"]
