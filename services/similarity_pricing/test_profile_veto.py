"""Weto na profilach liczbowych TypeSafe (services/typesafe_profile/weto.py).

Kontrakt jak weto słowne: sprzeczność stwierdzona po OBU stronach = against,
wszystko inne (brak danych, strefa szarości 0,2–0,8, zgodność) = abstain.
Progi z dokumentacji TypeSafe (TAK 0,8 / NIE 0,2), nie strojone na holdoucie.
"""
from services.similarity_pricing.layer_identity import (
    identity_votes,
    is_identity_match,
    related_demotion_reason,
    vote_taxonomy_axis,
)
from services.typesafe_profile import weto

MESKIE = {"odb:mezczyzni": 0.95, "odb:kobiety": 0.04, "odb:dzieci": 0.02}
DAMSKIE = {"odb:mezczyzni": 0.03, "odb:kobiety": 0.93, "odb:dzieci": 0.02}


def _s(profil=None, **kw):
    base = {"service_name": "Strzyżenie", "price_grosze": 10000, "duration_minutes": 30}
    base.update(kw)
    if profil is not None:
        base["_profil"] = profil
    return base


def test_odbiorca_sprzeczny_tylko_gdy_obie_strony_pewne():
    assert weto.odbiorca(MESKIE, DAMSKIE)
    # strefa szarości: salon nie mówi wprost, model waha się — brak weta
    niepewne = {"odb:mezczyzni": 0.6, "odb:kobiety": 0.4}
    assert not weto.odbiorca(MESKIE, niepewne)
    assert not weto.odbiorca(MESKIE, MESKIE)


def test_dlugosc_wymaga_obecnosci_i_roznicy_poltora_poziomu():
    krotkie = {"dl:obecna": 0.95, "dl:skala": 1.0}
    dlugie = {"dl:obecna": 0.95, "dl:skala": 2.98}  # v1 z progiem 2,0 tego nie łapało
    srednie = {"dl:obecna": 0.95, "dl:skala": 2.0}
    assert weto.dlugosc(krotkie, dlugie)
    assert not weto.dlugosc(krotkie, srednie)  # sąsiednie poziomy salony rozgraniczają różnie
    bez_dlugosci = {"dl:obecna": 0.1, "dl:skala": 4.0}  # skala bez obecności nic nie znaczy
    assert not weto.dlugosc(krotkie, bez_dlugosci)


def test_skala_bez_wartosci_nie_wywraca():
    # mapa pytań mogła wyciąć skalę w danej branży — brak klucza = brak weta, nie KeyError
    assert not weto.zakres({"zakres:obecny": 0.9}, {"zakres:obecny": 0.9, "zakres:skala": 3.0})


def test_obszar_rozlaczne_okolice():
    rzesy = {"ob:rzesy": 0.97, "ob:brwi": 0.03}
    brwi = {"ob:rzesy": 0.05, "ob:brwi": 0.96}
    henna_obu = {"ob:rzesy": 0.9, "ob:brwi": 0.9}
    assert weto.obszar(rzesy, brwi)
    assert not weto.obszar(rzesy, henna_obu)  # część wspólna — to może być ta sama usługa


def test_etap_zalozenie_vs_uzupelnienie():
    zalozenie = {"etap:zalozenie": 0.95, "etap:uzupelnienie": 0.03}
    uzupelnienie = {"etap:zalozenie": 0.04, "etap:uzupelnienie": 0.94}
    assert weto.etap(zalozenie, uzupelnienie)


def test_metoda_wymaga_pewnosci_i_pomija_inna():
    a = {"metoda": "balejaz_ombre", "metoda#pewnosc": 0.9}
    b = {"metoda": "koloryzacja_globalna", "metoda#pewnosc": 0.92}
    assert weto.metoda(a, b)
    assert not weto.metoda(a, {"metoda": "inna", "metoda#pewnosc": 0.99})
    assert not weto.metoda(a, {"metoda": "koloryzacja_globalna", "metoda#pewnosc": 0.6})


def test_os_rozmiar_silnika_to_objetosc_zakres_pakiet():
    klasyczne = {"obj:obecna": 0.95, "obj:skala": 0.0}
    objetosciowe = {"obj:obecna": 0.95, "obj:skala": 2.1}
    assert weto.axis_conflict(klasyczne, objetosciowe, "rozmiar")
    pakiet = {"pakiet": 0.93}
    pojedyncza = {"pakiet": 0.05}
    assert weto.axis_conflict(pakiet, pojedyncza, "rozmiar")
    assert not weto.axis_conflict(pakiet, pojedyncza, "nieznana_os")


def test_vote_na_profilach_ignoruje_osie_slowne():
    a = _s(MESKIE, _tax={"odbiorca": "damskie"})
    b = _s(MESKIE, _tax={"odbiorca": "męskie"})
    # słownie sprzeczne, liczbowo zgodne — liczą się profile
    assert vote_taxonomy_axis(a, b, "odbiorca") == "abstain"
    assert vote_taxonomy_axis(_s(MESKIE), _s(DAMSKIE), "odbiorca") == "against"


def test_brak_profilu_po_jednej_stronie_to_abstain():
    assert vote_taxonomy_axis(_s(MESKIE), _s(), "odbiorca") == "abstain"
    assert vote_taxonomy_axis(_s(MESKIE), _s({}), "odbiorca") == "abstain"


def test_konflikt_profilu_jest_twardym_wetem():
    votes = identity_votes(_s(MESKIE), _s(DAMSKIE))
    assert votes["tax_odbiorca"] == "against"
    assert is_identity_match(votes, strictness=0.0) is False


def test_metoda_z_profilu_tylko_degraduje():
    a = _s({"metoda": "balejaz_ombre", "metoda#pewnosc": 0.9})
    b = _s({"metoda": "koloryzacja_globalna", "metoda#pewnosc": 0.9})
    assert is_identity_match(identity_votes(a, b), strictness=0.0) is True
    assert related_demotion_reason(a, b) == "metoda"


def test_bez_profili_i_osi_wynik_jak_dotad():
    votes = identity_votes(_s(), _s())
    assert all(v == "abstain" for k, v in votes.items() if k.startswith("tax_"))
