"""Bateria pytań badania usługi (services/typesafe_profile/pytania.py) — kontrakt.

Pilnuje spójności trzech rzeczy, które zmieniają się niezależnie: pytań,
mapy pytań per branża (mapa_pytan.json) i reguł weta (weto.py). Rozjazd
oznaczałby, że weto czyta klucz, o który nikt nie pyta — i milczy po cichu.
"""
from types import SimpleNamespace

from typesafe_sdk import Choice, Noul, Score

from services.typesafe_profile import pytania, weto
from services.typesafe_profile.listy import METODA


def test_mapa_zawiera_wylacznie_istniejace_pytania():
    for branza, klucze in pytania.MAPA_PYTAN.items():
        wszystkie = set(pytania.build_questions(branza))
        assert klucze <= wszystkie, f"{branza}: {sorted(klucze - wszystkie)}"


def test_mapa_zostawia_pytania_bazowe_w_kazdej_branzy():
    bazowe = {"odb:kobiety", "odb:mezczyzni", "odb:dzieci", "zakres:obecny", "zakres:skala", "pakiet"}
    for branza, klucze in pytania.MAPA_PYTAN.items():
        assert bazowe <= klucze, branza


def test_branza_spoza_mapy_dostaje_komplet_bez_metody():
    q = pytania.questions_for_branch("Weterynaria")
    assert q.keys() == pytania.build_questions("Weterynaria").keys()
    assert "metoda" not in q  # brak listy technik = brak wyboru z listy


def test_typy_pytan():
    q = pytania.build_questions("Fryzjer")
    assert isinstance(q["odb:mezczyzni"], Noul)
    assert isinstance(q["dl:skala"], Score)
    assert isinstance(q["metoda"], Choice)


def test_kazda_lista_metod_ma_opcje_brak_dopasowania():
    for branza in METODA:
        kryteria = pytania.build_questions(branza)["metoda"].criteria
        assert "inna" in kryteria, branza


def test_klucze_czytane_przez_weto_istnieja_w_baterii():
    pytane = set(pytania.build_questions("Salon Kosmetyczny"))
    czytane = {k for para in (*weto.ODBIORCY_WYKLUCZAJACY, *weto.ETAPY_WYKLUCZAJACE) for k in para}
    czytane |= {"dl:obecna", "dl:skala", "zakres:obecny", "zakres:skala", "obj:obecna", "obj:skala", "pakiet", "metoda"}
    assert czytane <= pytane, sorted(czytane - pytane)


def test_profile_zamienia_odpowiedzi_na_liczby():
    wynik = SimpleNamespace(
        nouls={"odb:mezczyzni": SimpleNamespace(noul=0.91234)},
        scores={"dl:skala": SimpleNamespace(score=2.4567, confidence=0.7777)},
        choices={"metoda": SimpleNamespace(choice="strzyzenie", confidence=0.95)},
    )
    assert pytania.profile(wynik) == {
        "odb:mezczyzni": 0.912,
        "dl:skala": 2.457,
        "dl:skala#pewnosc": 0.778,
        "metoda": "strzyzenie",
        "metoda#pewnosc": 0.95,
    }


def test_stan_niesie_kategorie_tylko_gdy_jest():
    assert pytania.build_state("Strzyżenie", None, "Fryzjer") == {
        "usluga": {"nazwa": "Strzyżenie", "branza_salonu": "Fryzjer"}
    }
    assert pytania.build_state("Strzyżenie", "Panowie", "Fryzjer")["usluga"]["kategoria_w_cenniku"] == "Panowie"
