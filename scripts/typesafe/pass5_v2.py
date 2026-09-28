"""Pass 5 w wersji 2 — pytania zbudowane według zasad TypeSafe.

Wersja 1 zadawała jedno szerokie pytanie („która z 16 kategorii, w tym
własna”). Ukrywało ono kilka ocen naraz, mieszało „która” z „czy w ogóle”
i przez dosłowne czytanie odrzucało grupy bez metody (wynik: 50% zgodności
z gpt-4o, raport 250).

Wersja 2 (wzór: docs.typesafe.ai cookbooks/skill_suggestion, primitives/noul):

* `ktora` — wybór spośród SAMYCH kandydatów Booksy; jego rozkład to ranking.
* `pasuje::<tid>` — osobne tak/nie per kandydat: czy obejmuje zabiegi grupy.
* `metoda::<tid>` — konflikt metody; zadawane TYLKO, gdy grupa ma metodę.
* `okolica::<tid>` — konflikt okolicy; zadawane TYLKO, gdy grupa ma okolice.

Decyzję składa kod: kandydat się kwalifikuje, gdy pasuje i nie ma konfliktu;
wygrywa kwalifikujący się z najwyższym prawdopodobieństwem w `ktora`; brak
kwalifikujących się = własna kategoria salonu (reguła Pass 5 z prod: „lepsza
własna kategoria niż kategoria z inną metodą”).
"""

from __future__ import annotations

from typing import Any

OWN_CATEGORY = "wlasna_kategoria"

# 0,5 = model daje „tak” i „nie” równe szanse; docs primitives/noul: „Use 0.5
# when yes and no are equally easy to act on”. Progi NIE są strojone na
# żadnym raporcie (bd: twarda zasada uniwersalności silnika).
PROG_PASUJE = 0.5
PROG_KONFLIKT = 0.5
# Strefa szarości do przeglądu — przykład YES/NO z primitives/noul.
SZAROSC = (0.2, 0.8)

TEKSTY: dict[str, dict[str, Any]] = {
    "pl": {
        "k": {
            "grupa": "grupa", "marka": "marka_urzadzenia", "metoda": "metoda",
            "okolice": "okolice_ciala", "uslugi": "uslugi", "nazwa": "nazwa",
            "kat": "kategoria_w_cenniku", "kandydat": "kandydat",
            "kat_booksy": "kategoria", "dzial": "dzial",
        },
        "ktora": "Która kategoria Booksy najlepiej opisuje usługi z `grupa.uslugi`?",
        "pasuje": "Czy kategoria `kandydat` obejmuje zabiegi opisane przez usługi z `grupa.uslugi`?",
        "metoda": "Czy kategoria `kandydat` wskazuje metodę wykonania inną niż `grupa.metoda`?",
        "okolica": "Czy kategoria `kandydat` dotyczy innej okolicy ciała niż `grupa.okolice_ciala`?",
    },
    "en": {
        "k": {
            "grupa": "group", "marka": "device_brand", "metoda": "method",
            "okolice": "body_areas", "uslugi": "services", "nazwa": "name",
            "kat": "salon_menu_section", "kandydat": "candidate",
            "kat_booksy": "category", "dzial": "department",
        },
        "ktora": (
            "Which Booksy category best describes the services in `group.services`? "
            "Service and category names are in Polish."
        ),
        "pasuje": "Does the category `candidate` cover the treatments described by the services in `group.services`?",
        "metoda": "Does the category `candidate` name a treatment method different from `group.method`?",
        "okolica": "Does the category `candidate` refer to a body area different from `group.body_areas`?",
    },
}


def _readable(token: str) -> str:
    return token.replace("_", " ")


def build_state(key: tuple[str | None, str, tuple[str, ...]], members: list[dict[str, Any]],
                lang: str, max_members: int) -> tuple[dict[str, Any], bool, bool]:
    """Stan tylko z tym, czego pytania potrzebują: puste cechy grupy są pomijane."""
    k = TEKSTY[lang]["k"]
    brand, method, areas = key
    group: dict[str, Any] = {}
    if brand:
        group[k["marka"]] = brand
    has_method = bool(method) and method != "generic"
    if has_method:
        group[k["metoda"]] = _readable(method)
    has_area = bool(areas)
    if has_area:
        group[k["okolice"]] = [_readable(a) for a in areas]
    group[k["uslugi"]] = [
        {k["nazwa"]: m.get("name") or "", k["kat"]: m.get("category_name") or ""}
        for m in members[:max_members]
    ]
    return {k["grupa"]: group}, has_method, has_area


def _candidate_record(c: dict[str, Any], lang: str) -> dict[str, Any]:
    k = TEKSTY[lang]["k"]
    record = {k["kat_booksy"]: c["canonical_name"]}
    if c.get("parent_canonical_name"):
        record[k["dzial"]] = c["parent_canonical_name"]
    return record


def build_questions(candidates: list[dict[str, Any]], lang: str,
                    has_method: bool, has_area: bool) -> dict[str, Any]:
    t = TEKSTY[lang]
    questions: dict[str, Any] = {}
    if len(candidates) > 1:
        questions["ktora"] = {
            "type": "choice",
            "instructions": t["ktora"],
            "criteria": {f"tid_{c['tid']}": _candidate_record(c, lang) for c in candidates},
        }
    for c in candidates:
        record = {t["k"]["kandydat"]: _candidate_record(c, lang)}
        questions[f"pasuje::{c['tid']}"] = {"type": "noul", "instructions": {**record, "question": t["pasuje"]}}
        if has_method:
            questions[f"metoda::{c['tid']}"] = {"type": "noul", "instructions": {**record, "question": t["metoda"]}}
        if has_area:
            questions[f"okolica::{c['tid']}"] = {"type": "noul", "instructions": {**record, "question": t["okolica"]}}
    return questions


def decide(answers: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """Złożenie odpowiedzi w decyzję — cała polityka jest tutaj, w kodzie."""
    ranking = answers.get("ktora", {}).get("probabilities") or {f"tid_{candidates[0]['tid']}": 1.0}
    per: dict[str, dict[str, Any]] = {}
    for c in candidates:
        tid = c["tid"]
        fit = answers[f"pasuje::{tid}"]["noul"]
        method = answers.get(f"metoda::{tid}", {}).get("noul")
        area = answers.get(f"okolica::{tid}", {}).get("noul")
        per[f"tid_{tid}"] = {
            "nazwa": c["canonical_name"],
            "ranking": round(ranking.get(f"tid_{tid}", 0.0), 3),
            "pasuje": fit,
            "konflikt_metody": method,
            "konflikt_okolicy": area,
            "kwalifikuje": fit >= PROG_PASUJE
            and (method is None or method < PROG_KONFLIKT)
            and (area is None or area < PROG_KONFLIKT),
        }
    eligible = [k for k, v in per.items() if v["kwalifikuje"]]
    if eligible:
        choice = max(eligible, key=lambda k: per[k]["ranking"])
        certainty = per[choice]["pasuje"]
    else:
        choice = OWN_CATEGORY
        certainty = 1.0 - max(v["pasuje"] for v in per.values())
    lo, hi = SZAROSC
    top = sorted(per.items(), key=lambda kv: -kv[1]["ranking"])[:3]
    return {
        "wybor": choice,
        "pewnosc": round(certainty, 3),
        "szara_strefa": lo < certainty < hi,
        "kwalifikujacych": len(eligible),
        "top3_rankingu": [{"klucz": k, **v} for k, v in top],
    }
