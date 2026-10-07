"""Promocja wpisana w nazwę usługi.

Booksy ma promocję strukturalną (variants[].promotion), ale część salonów
(ok. 2,1 tys. z 48,8 tys., pomiar 10.2026) wpisuje promocję tylko w nazwę:
„Depilacja laserowa bikini -20%”, „Koktajl MONACO – promocja miesiąca”.
Bez tego sygnału monitoring konkurencji ich nie widzi.

Zakres liczb: „-20%” po literze lub spacji to rabat, ale „70-80%” / „20-40%”
(krycie, stężenie kwasu) to przedział — myślnik po cyfrze lub % nie liczy się.
"""

from __future__ import annotations

import re

_PROMO_W_NAZWIE = re.compile(
    r"promocj"
    r"|\bpromo\b"
    r"|rabat"
    r"|zni[żz]k"
    r"|(?<![\d%])-\s?\d{1,2}\s?%"
    r"|\d{1,2}\s?%\s?(?:taniej|zni|rabat|off)",
    re.IGNORECASE,
)


def promocja_w_nazwie(name: str | None) -> bool:
    return bool(name) and bool(_PROMO_W_NAZWIE.search(name))
