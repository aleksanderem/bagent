"""Weto osi taksonomii (TAXONOMY_VETO_AXES) — kontrakt z pomiaru holdoutu mig 188.

Zwycięski wariant: 5 osi rozdzielających jako TWARDE weto, metoda POZA wetem.
Pomiar (563 pary, 2 sędziów + kalibracja człowiekiem): 25% złych par wyciętych,
0/69 tożsamych straconych.
"""
from services.similarity_pricing.layer_identity import (
    TAXONOMY_VETO_AXES,
    identity_votes,
    is_identity_match,
    vote_taxonomy_axis,
)


def _s(tax=None, **kw):
    base = {"service_name": "Strzyżenie", "price_grosze": 10000, "duration_minutes": 30}
    base.update(kw)
    if tax is not None:
        base["_tax"] = tax
    return base


def test_konflikt_odbiorcy_to_against():
    # strzyżenie męskie vs damskie — klasa błędu nr 1 z pomiaru trafności
    a = _s(tax={"odbiorca": "męskie"})
    b = _s(tax={"odbiorca": "damskie"})
    assert vote_taxonomy_axis(a, b, "odbiorca") == "against"


def test_zgodnosc_to_abstain_nigdy_for():
    # lekcja Stylage: taksonomia wyłącznie ZAPRZECZA — zgodna wartość nie buduje klastra
    a = _s(tax={"obszar": "twarz"})
    b = _s(tax={"obszar": "Twarz"})
    assert vote_taxonomy_axis(a, b, "obszar") == "abstain"


def test_brak_danych_to_abstain():
    # weto działa tylko tam, gdzie OBIE strony zdestylowane — raport bez pokrycia
    # zachowuje się identycznie jak przed wetem
    assert vote_taxonomy_axis(_s(tax={"rozmiar": "3 ml"}), _s(), "rozmiar") == "abstain"
    assert vote_taxonomy_axis(_s(), _s(), "etap") == "abstain"


def test_wspolny_rdzen_znosi_konflikt():
    # "laser tulowy i radiofrekwencja" vs "laser tulowy" — rodzina, nie sprzeczność
    a = _s(tax={"obszar": "cała twarz"})
    b = _s(tax={"obszar": "twarz i szyja"})
    assert vote_taxonomy_axis(a, b, "obszar") == "abstain"


def test_konflikt_osi_taksonomii_jest_twardym_wetem():
    # sam konflikt tnie parę nawet przy maksymalnie liberalnej surowości,
    # gdy wszystkie pozostałe osie się wstrzymują
    a = _s(tax={"etap": "przedłużanie"})
    b = _s(tax={"etap": "uzupełnienie"})
    votes = identity_votes(a, b)
    assert votes["tax_etap"] == "against"
    assert is_identity_match(votes, strictness=0.0) is False


def test_bez_taksonomii_wynik_jak_dotad():
    a, b = _s(), _s()
    votes = identity_votes(a, b)
    assert all(votes[f"tax_{ax}"] == "abstain" for ax in TAXONOMY_VETO_AXES)
    assert is_identity_match(votes, strictness=0.0) is True
