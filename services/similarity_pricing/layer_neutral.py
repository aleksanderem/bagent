"""Klasyfikacja kategorii NEUTRALNYCH — kategoria, która nie wskazuje domeny usługi.

Niektóre kategorie właścicieli nie niosą informacji o tożsamości zabiegu i nie
mogą głosować w teście tożsamości (layer_identity.vote_category je wstrzymuje).
Dwa typy, rozróżnione bo działają inaczej (data-driven, prod 2026-06-22):

  'service' — neutralna USŁUGOWO: zbiera de facto tę samą usługę z różnych domen.
              Przykłady: "Konsultacja", "Konsultacje", "Pakiety", "Voucher",
              "Bon podarunkowy", "Diagnostyka", "Pierwsza wizyta". Sama usługa
              (np. konsultacja) bywa tożsama, ale kategoria nie wskazuje domeny.
              NEUTRALNA TYLKO gdy goła — "Konsultacja kosmetologiczna" albo
              "Pakiety depilacji laserowej" mają domenę => NIE neutralne.

  'event'   — neutralna EVENTOWO: z natury zbiera RÓŻNE usługi (parasol
              promocyjny/sezonowy). Przykłady: "Promocje", "Oferta specjalna",
              "Popularne usługi", "Bestsellery", "Nowości", "Wiosenne promocje".
              Mocny marker eventowy dominuje NAWET z domeną ("Promocja czerwiec
              ... endermologia" to wciąż event). Słaby marker ("dla niej") jest
              eventem TYLKO gdy goły — "Depilacja laserowa dla niej" to depilacja.

None => kategoria domenowa (normalna, głosuje w teście tożsamości).

Funkcje czyste, stdlib only.
"""
from __future__ import annotations

import re

from .layer_category import _normalize  # wspólna normalizacja (lower/ascii/bez emoji)

# Mocne markery eventowe — parasol; dominują nad ewentualną domeną w nazwie.
_EVENT_STRONG = re.compile(
    r"\b(promocj\w*|promoc\w*|wyprzedaz\w*|bestseller\w*|nowosc\w*|nowosci|"
    r"polecane|popularne|oferta\w*|hit|hity|sale)\b"
)
# "okazja"/"okazje" jako event — ale NIE "okazjonalny" (to typ makijażu/zabiegu).
_EVENT_OKAZJA = re.compile(r"\bokazj[ae]\b")
# Słabe markery — eventem tylko gdy stanowią CAŁĄ (gołą) kategorię.
_EVENT_WEAK_EXACT = frozenset({
    "dla niej", "dla niego", "dla niej i dla niego", "dla niego i dla niej",
    "last minute", "happy hour", "dla niego i niej", "dla niej i niego",
})

# Rdzenie usługowo-neutralne (po normalizacji, bez diakrytyków).
_SERVICE_ROOTS = (
    "konsultacj", "pakiet", "voucher", "vouchery", "bon", "diagnostyk",
    "pierwsza wizyt", "wizyt", "porad", "dobor", "spotkani",
)

# Znane rdzenie DOMEN — jeśli zostają w nazwie po usunięciu rdzenia neutralnego,
# kategoria ma domenę => NIE jest neutralna usługowo. Rdzenie traktowane jako
# PREFIKSY (granica początku słowa + dowolna końcówka), żeby łapać odmiany
# ("kosmetolog" → "kosmetologiczna", "depilacj" → "depilacji").
_DOMAIN_MARKERS = re.compile(
    r"\b(manicure|pedicure|paznokc|depilacj|laser|wosk|rzes|brwi|brew|"
    r"fryzjer|koloryzacj|strzyzeni|barber|wlos|makijaz|permanentn|"
    r"medycyn|estetyczn|kosmetolog|kosmetyk|mezoterapi|botoks|botox|"
    r"kwas|wypelniacz|stymulator|peeling|masaz|twarz|cialo|stop|"
    r"dlon|sylwetk|podolog|trycholog|fizjoterapi|piercing|oprawa oka|"
    r"karboksy|endermolog|hifu|lifting|mikrodermabr|oczyszczani)"
)

# Słowa do usunięcia przy ocenie "reszty" po rdzeniu usługowym (generyczne, nie domena).
_SERVICE_FILLER = re.compile(
    r"\b(zabieg\w*|uslug\w*|podarunkow\w*|premium|spa|slubn\w*|"
    r"promocyjn\w*|pierwsz\w*|indywidualn\w*|wstepn\w*|i|na|dla|w|z|do|oraz|"
    r"online|stacjonarn\w*|bezplatn\w*|gratis|darmow\w*)\b"
)


def classify_neutral_category(category_name: str | None) -> str | None:
    """Zwróć 'event' / 'service' / None dla kategorii właściciela.

    None => kategoria domenowa (głosuje normalnie w teście tożsamości).
    'event'/'service' => neutralna (oś kategoria się wstrzymuje).
    """
    n = _normalize(category_name or "")
    if not n:
        return None  # pusta i tak abstain w voterze

    # --- EVENT ---
    # mocny marker => event nawet z domeną (parasol promocyjny)
    if _EVENT_STRONG.search(n) or _EVENT_OKAZJA.search(n):
        return "event"
    # słaby marker => event tylko gdy goły
    if n in _EVENT_WEAK_EXACT:
        return "event"

    # --- SERVICE ---
    # rdzeń usługowo-neutralny + brak domeny w reszcie
    for root in _SERVICE_ROOTS:
        if root in n:
            rest = re.sub(re.escape(root) + r"\w*", " ", n)
            # jeśli w reszcie jest znana domena => NIE neutralna usługowo
            if _DOMAIN_MARKERS.search(rest):
                return None
            # usuń generyczne wypełniacze; jeśli nic istotnego nie zostaje => service
            rest = _SERVICE_FILLER.sub(" ", rest)
            rest = re.sub(r"\s+", " ", rest).strip()
            if not rest:
                return "service"
            # krótka resztka bez domeny też uznajemy za neutralną usługowo
            if len(rest.split()) <= 1:
                return "service"
    return None


def is_neutral_category(category_name: str | None) -> bool:
    """Czy kategoria jest neutralna (dowolnego typu) — wygodny skrót dla votera."""
    return classify_neutral_category(category_name) is not None
