"""Badanie usługi — destylacja jako komplet pomiarów TypeSafe, nie etykieta.

Zamiast wyciągać słowo do każdej cechy i porównywać słowa (GLM: „męski” vs
„mężczyzna” = fałszywe weto), mierzymy usługę trzema typami pytań naraz
i przechowujemy LICZBY. Porównanie dwóch usług robi kod (weto.py)
z tolerancjami — progi zmienia się bez ponownego pytania modelu.

* Score  — wymiary uporządkowane: długość, zakres obszaru, objętość rzęs. Skala
           nie ma opcji „brak”, więc obok stoi tak/nie „czy nazwa w ogóle to
           podaje” (docs: „use a separate presence judgment”).
* Noul   — własności tak/nie, kilka niezależnych: odbiorca, etap, okolice,
           pakiet, dodatek. „Nie wiadomo” = niskie wartości, bez sztucznej opcji.
* Choice — jedyne miejsce na listę: główna technika w obrębie branży
           (listy.METODA), z opcją „inna”.

Wszystkie pytania idą w JEDNYM wywołaniu na nazwę (oceniane niezależnie).
Mapa pytań (mapa_pytan.json) zostawia w branży tylko pytania, które jej
dotyczą — pomiar 21.09: −9,2% tokenów, 0 zmienionych decyzji weta na 700 parach.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from typesafe_sdk import Choice, Noul, NoulCriteria, Score

from .listy import METODA, OBSZAR

U = "`usluga`"

# Wersja baterii pytań. Zmiana treści pytania albo listy = nowa wersja, bo
# odpowiedzi przestają być porównywalne; service_profile trzyma ją w kluczu,
# więc stare profile nie mieszają się z nowymi.
WERSJA = 2

# v2: bez słowa „wyraźnie”. Jev czyta dosłownie — w v1 „Strzyżenie włosów” w
# barber shopie dostawało 0,62 dla mężczyzn, bo nazwa nie mówi tego WPROST,
# choć typ salonu mówi. Sędziowie holdoutu (i klientka) biorą salon pod uwagę.
KONTEKST = "biorąc pod uwagę nazwę, kategorię i typ salonu"
ODBIORCY = {
    "kobiety": f"Czy usługa {U} jest przeznaczona dla kobiet, {KONTEKST}?",
    "mezczyzni": f"Czy usługa {U} jest przeznaczona dla mężczyzn, {KONTEKST}?",
    "dzieci": f"Czy usługa {U} jest przeznaczona dla dzieci, {KONTEKST}?",
    "para": f"Czy usługa {U} jest wykonywana dla dwóch osób jednocześnie?",
    "ciaza": f"Czy usługa {U} jest przeznaczona dla kobiet w ciąży?",
    "zwierze": f"Czy usługa {U} jest wykonywana na zwierzęciu?",
}

ETAPY = {
    "zalozenie": f"Czy usługa {U} to pierwsze założenie lub nowe przedłużenie paznokci, rzęs albo włosów?",
    "uzupelnienie": f"Czy usługa {U} to uzupełnienie lub dopełnienie wcześniejszej stylizacji?",
    "zdjecie": f"Czy usługa {U} obejmuje zdjęcie lub usunięcie wcześniejszej stylizacji?",
    "korekta": f"Czy usługa {U} to korekta, poprawka lub naprawa wcześniejszej usługi?",
    "konsultacja": f"Czy usługa {U} to sama konsultacja, bez wykonania zabiegu?",
}

DLUGOSC_POZIOMY = [
    "bardzo krótkie — np. strzyżenie na zero, na łyso",
    "krótkie — np. włosy krótkie, krótkie paznokcie, krótkie rzęsy",
    "średnie — np. włosy do ramion, półdługie, średnia długość paznokci",
    "długie — np. włosy do łopatek, długie paznokcie",
    "bardzo długie — np. włosy do pasa, bardzo długie paznokcie",
]

OBJETOSC_POZIOMY = [
    "klasyczne rzęsy 1:1 — jedna sztuczna rzęsa na jedną naturalną",
    "lekka objętość — 2D lub 3D, np. 1:2, 1:3",
    "duża objętość — 4D do 6D, np. 1:4, 4-5D",
    "mega volume — 7D i więcej",
]

ZAKRES_POZIOMY = [
    "jedna drobna partia — np. wąsik, pachy, płatek ucha, brwi",
    "jedna partia ciała — np. cała twarz, łydki, broda, plecy",
    "kilka partii naraz — np. twarz, szyja i dekolt; włosy i broda; dłonie i stopy",
    "całe ciało",
]

# Mapa: branża salonu -> klucze pytań, które mają w niej sens. Generuje ją
# scripts/typesafe/mapa_pytan.py (próg: cecha wspomniana w ≥ 1 na 1000 nazw
# branży). Branża spoza mapy dostaje komplet pytań.
MAPA_PYTAN: dict[str, frozenset[str]] = {
    branza: frozenset(klucze)
    for branza, klucze in json.loads(
        Path(__file__).with_name("mapa_pytan.json").read_text(encoding="utf-8")
    ).items()
}


def build_state(name: str, category: str | None, branch: str) -> dict[str, Any]:
    usluga: dict[str, Any] = {"nazwa": name, "branza_salonu": branch}
    if category:
        usluga["kategoria_w_cenniku"] = category
    return {"usluga": usluga}


def build_questions(branch: str, allowed: set[str] | frozenset[str] | None = None) -> dict[str, Any]:
    """Komplet pytań; `allowed` zostawia tylko wskazane klucze."""
    q = _all_questions(branch)
    return q if allowed is None else {k: v for k, v in q.items() if k in allowed}


def questions_for_branch(branch: str) -> dict[str, Any]:
    """Pytania dla branży według mapy (branża spoza mapy = komplet)."""
    return build_questions(branch, MAPA_PYTAN.get(branch))


def _all_questions(branch: str) -> dict[str, Any]:
    q: dict[str, Any] = {}
    for key, text in ODBIORCY.items():
        q[f"odb:{key}"] = Noul(instructions=text)
    q["dl:obecna"] = Noul(
        instructions=f"Czy nazwa lub kategoria {U} określa długość włosów, paznokci albo rzęs?",
        criteria=NoulCriteria(
            true="podaje długość, np. włosy długie, do ramion, krótkie paznokcie, długość 1-2",
            false="nie podaje długości; czas trwania zabiegu (np. 90 min) to nie długość",
        ),
    )
    q["dl:skala"] = Score(
        instructions=f"Jaka jest długość włosów, paznokci lub rzęs, których dotyczy usługa {U}?",
        criteria=DLUGOSC_POZIOMY,
    )
    q["zakres:obecny"] = Noul(
        instructions=f"Czy nazwa lub kategoria {U} wskazuje, na jakiej części ciała wykonuje się usługę?",
    )
    q["zakres:skala"] = Score(
        instructions=f"Jak duży obszar ciała obejmuje usługa {U}?",
        criteria=ZAKRES_POZIOMY,
    )
    q["obj:obecna"] = Noul(
        instructions=f"Czy nazwa lub kategoria {U} określa objętość przedłużanych rzęs?",
        criteria=NoulCriteria(
            true="podaje objętość, np. 1:1, 2D, 3-4D, 1:4, mega volume",
            false="nie podaje objętości albo usługa nie dotyczy przedłużania rzęs",
        ),
    )
    q["obj:skala"] = Score(
        instructions=f"Jaką objętość przedłużanych rzęs obejmuje usługa {U}?",
        criteria=OBJETOSC_POZIOMY,
    )
    for key, desc in OBSZAR.items():
        if isinstance(desc, tuple):
            text, not_this = desc
            q[f"ob:{key}"] = Noul(
                instructions=f"Czy usługa {U} jest wykonywana w obszarze: {text}?",
                criteria=NoulCriteria(true=text, false=not_this),
            )
        else:
            q[f"ob:{key}"] = Noul(instructions=f"Czy usługa {U} jest wykonywana w obszarze: {desc}?")
    for key, text in ETAPY.items():
        q[f"etap:{key}"] = Noul(instructions=text)
    q["pakiet"] = Noul(instructions=f"Czy usługa {U} to pakiet kilku sesji albo kilku zabiegów sprzedawanych razem?")
    q["dodatek"] = Noul(instructions=f"Czy usługa {U} to dodatek dokupowany do innej usługi?")
    methods = METODA.get(branch)
    if methods:
        q["metoda"] = Choice(
            instructions=f"Jaką główną technikę wykonuje się w usłudze {U}?",
            criteria={
                **methods,
                "inna": {
                    "what": "technika spoza tej listy albo nazwa jej nie wskazuje",
                    "not_for": "technika opisana którąś z pozostałych opcji",
                },
            },
        )
    return q


def profile(result: Any) -> dict[str, Any]:
    """Odpowiedzi → profil liczbowy (jedyne, co zapisujemy)."""
    prof: dict[str, Any] = {k: round(a.noul, 3) for k, a in result.nouls.items()}
    for k, a in result.scores.items():
        prof[k] = round(a.score, 3)
        prof[f"{k}#pewnosc"] = round(a.confidence, 3)
    choice = result.choices.get("metoda")
    if choice is not None:
        prof["metoda"] = choice.choice
        prof["metoda#pewnosc"] = round(choice.confidence, 3)
    return prof
