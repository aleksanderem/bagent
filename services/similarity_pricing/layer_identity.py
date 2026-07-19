"""Drugie prawo — wielo-osiowy test TOŻSAMOŚCI usług.

CEL CAŁOŚCI: znaleźć usługi TOŻSAME (ta sama usługa u innych salonów) mimo różnic
w nazwach i natywnych taksonomiach. Embedding nazwy (fn_find_related_v2) daje
wstępny klaster — pierwszy sygnał tożsamości, ale niewystarczający (na nim
"konsultacja" myli domeny). Ta warstwa to drugi poziom: każdy bliźniak jest
testowany na WIELU OSIACH naraz, czy to faktycznie ta sama usługa.

Zasada głosowania (każda oś osobno):
  'for'     — oś potwierdza tożsamość (silny dowód że to samo)
  'against' — oś przeczy tożsamości (dowód że to inna usługa)
  'abstain' — oś nie wie (pusty/brak sygnału) → NIE liczy się; pusta kategoria
              nigdy nie jest powodem odrzucenia ("nie wiem" ≠ "obce").

Osie i ich siła (waga) — dobrane wg tego JAK MOCNO oś rozróżnia tożsamość:
  params   (±1.0) — parametry zabiegu (ml, liczba, obszar). "0,5 ml" vs "2 ml" to
                    RÓŻNA usługa — najsilniejszy dyskryminator. Zgodne = silne 'for'.
  package  (±1.0) — pakiet vs pojedyncza to różna usługa. Różny status = 'against';
                    zgodny = 'abstain' (zgodność pakietu to stan domyślny, nie dowód).
  category (±0.6) — kategoria właściciela. SŁABSZA, bo właściciele niespójnie
                    kategoryzują to samo (presoterapia w "Pielęgnacji ciała" i
                    "Modelowaniu sylwetki" to wciąż presoterapia). Zgodna = 'for',
                    rozłączna (obie obecne, zero wspólnych tokenów) = 'against',
                    pusta = 'abstain'.
  duration (-0.5) — czas. Tylko SKRAJNA różnica (>=3×, np. 15 vs 60 min) głosuje
                    'against'; podobny czas = 'abstain' (ten sam zabieg bywa różnie
                    długi). Czas nie potwierdza tożsamości, tylko sygnalizuje obcość.

Surowość (strictness 0..1) steruje progiem: ile przewagi 'against' tolerujemy
zanim odrzucimy bliźniaka. To jest "pokrętło" — sterowane adaptacyjnie (patrz
adaptive_identity_filter) lub ręcznie. Funkcje czyste, immutable, stdlib only.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

from ..body_area_taxonomy import extract_body_areas
from .layer_category import is_generic_name
from .layer_neutral import is_neutral_category

# Wagi osi — siła dyskryminacji tożsamości. Asymetryczne kierunki opisane w voterach.
# UWAGA: waga kategorii jest DYNAMICZNA (patrz _category_weight_for) — zależy od
# generyczności nazwy subjectu. Wartość tu to fallback gdy generyczność nieznana.
AXIS_WEIGHTS: dict[str, float] = {
    "params": 1.0,
    "package": 1.0,
    "body_area": 1.0,   # różny zakres obszarów = inna usługa (twarz vs twarz+szyja+dekolt)
    "price": 1.5,       # rozrzut ceny rzędu wielkości — mocny sygnał innej usługi
    "category": 0.6,
    "duration": 0.5,
}

# Oś PRICE — asymetryczna. NIGDY 'for' (cena nie buduje klastra, jest wynikiem).
# 'against' tylko gdy ceny (zł/min) rozjeżdżają się o ten współczynnik — sygnał że
# pod zgodną nazwą siedzą różne zabiegi (laser nieablacyjny 600 vs frakcyjny 3667).
# Ostrożny próg: nie karze normalnej wariancji rynkowej / premium (±do ~4×).
PRICE_RATIO_AGAINST = 4.0

# Waga osi kategoria zależnie od nazwy subjectu:
#   nazwa GENERYCZNA ("Konsultacja") — sama nazwa nie niesie tożsamości, więc
#     kategoria DECYDUJE: rozłączna kategoria = mocny dowód innej usługi.
#   nazwa SPECYFICZNA ("Presoterapia drenaż") — embedding nazwy już niesie
#     tożsamość, kategoria jest pomocnicza: różna kategoria to częściej
#     niespójność właściciela niż inna usługa, więc waży mało (nie wycina tożsamych).
CAT_WEIGHT_GENERIC = 2.0
CAT_WEIGHT_SPECIFIC = 0.4


def _category_weight_for(subject: dict[str, Any]) -> float:
    """Waga osi kategoria dla danego subjectu — wysoka gdy nazwa generyczna."""
    return CAT_WEIGHT_GENERIC if is_generic_name(subject.get("service_name") or "") else CAT_WEIGHT_SPECIFIC

# Próg czasu: stosunek dłuższy/krótszy >= tej wartości => 'against'.
_DURATION_RATIO_AGAINST = 3.0


# ---------------------------------------------------------------------------
# Normalizacja tekstu (współdzielona z layer_category konwencja)
# ---------------------------------------------------------------------------

def _normalize_text(text: str | None) -> str:
    """Lower-case ASCII bez diakrytyków, bez emoji/prefiksów porządkowych."""
    if not text:
        return ""
    text = re.sub(r"^\s*[\dIVXivx]+\.\s*", "", text)
    text = re.sub(r"[^\w\s,.]", " ", text, flags=re.UNICODE)
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_text).strip().lower()


# ---------------------------------------------------------------------------
# Ekstrakcja parametrów zabiegu z nazwy (oś params — najsilniejsza)
# ---------------------------------------------------------------------------

# ml: "0,5 ml", "1ml", "2 ml", "1.0 ml"
_RE_ML = re.compile(r"(\d+(?:[.,]\d+)?)\s*ml\b")
# liczba sztuk/jednostek: "5 paznokci", "1 okolica", "10 zabiegow", "3 partie"
_RE_COUNT = re.compile(
    r"\b(\d+)\s*(paznok\w*|okolic\w*|stref\w*|parti\w*|punkt\w*|zabieg\w*|szt\w*|obszar\w*|ampul\w*)"
)
# --- 2026-07-19: uniwersalne tokeny numeryczne (sweep wykazał mixing wariantów) ---
# WOLUMEN stylizacji: "1:1" / "2:1" / "2D" / "3D" / "3/4D" — to TEN SAM wymiar
# (ilość włosków na rzęsę naturalną: 2D ≡ 2:1), więc normalizujemy do wspólnego
# klucza "volume" (zakres [lo, hi]); sprzeczny wolumen = twarde weto osi params.
# Dowód z prod (sweep 2026-07-19): "Uzupełnienie rzęs 2:1" klastrował z
# 13×"Uzupełnienie rzęs 1:1" (fallback 0.75); "Przedłużanie rzęs 1:1" ↔ "2:1".
# Wzorce są czysto NUMERYCZNE — działają dla każdej przyszłej notacji x:y / nD,
# bez słownika zabiegów (filozofia: system działa nie wiedząc, co czyta).
_RE_RATIO = re.compile(r"\b(\d{1,2})\s*:\s*(\d{1,2})\b")
_RE_DIM   = re.compile(r"\b(\d{1,2})(?:\s*/\s*(\d{1,2}))?\s*d\b")
# OKRES ważności/uzupełnienia: "do 3 tyg", "po 4 tygodniach" — różny okres
# refillu to inna usługa cenowo (dowód: "3:1 do 4 tyg" ↔ "3:1 do 3 tyg" w sweep).
_RE_WEEKS = re.compile(r"\b(\d{1,2})\s*(?:tyg\w*|tydz\w*)")
# DŁUGOŚĆ (paznokcie/rzęsy): "długość 1-2", "dlugosc 3" — rozjazd = inny zakres.
_RE_LENGTH = re.compile(r"dlugosc\w*\s*(\d{1,2})(?:\s*-\s*(\d{1,2}))?")


def extract_params(name: str | None) -> dict[str, Any]:
    """Wyciągnij parametry zabiegu z nazwy.

    Zwraca dict z kluczami które WYSTĄPIŁY (puste gdy brak parametrów):
      "ml": float          — ilość ml (np. 0.5)
      "count": (int, str)  — (liczba, jednostka znormalizowana), np. (5, "paznok")

    Pusty dict = brak rozpoznanych parametrów (oś params się wstrzyma).
    """
    norm = _normalize_text(name)
    # Fold dla wzorców numerycznych: diakrytyki→ASCII (ł→l), lowercase, ale
    # ZACHOWUJE ':' '/' '-' — _normalize_text wycina interpunkcję, przez co
    # "1:1" stawało się "1 1" (nie do odróżnienia od przypadkowych liczb),
    # a "długość" traciła 'ł' zanim NFKD mógł ją złożyć ("dugosc").
    raw = (name or "").translate(str.maketrans({"ł": "l", "Ł": "L"}))
    import unicodedata as _ud
    raw = "".join(c for c in _ud.normalize("NFKD", raw) if not _ud.combining(c)).lower()
    out: dict[str, Any] = {}
    m = _RE_ML.search(norm)
    if m:
        out["ml"] = float(m.group(1).replace(",", "."))
    c = _RE_COUNT.search(norm)
    if c:
        # jednostkę skracamy do rdzenia (paznokcie/paznokci -> "paznok")
        unit = c.group(2)[:6]
        out["count"] = (int(c.group(1)), unit)
    # --- wolumen: ratio x:y i notacja nD sprowadzone do wspólnego zakresu ---
    # "1:1"→(1,1); "2:1"→(2,2); "3D"→(3,3); "3/4D"→(3,4). Porównanie w
    # vote_params: zakresy ROZŁĄCZNE => sprzeczność (against); nakładające
    # się (np. 3D vs 3/4D) => zgodne. Uwaga: ratio ma pierwszeństwo przed nD,
    # oba obecne — bierzemy ratio (dokładniejsze).
    r = _RE_RATIO.search(raw)
    if r:
        out["volume"] = (int(r.group(1)), int(r.group(1)))
    else:
        d = _RE_DIM.search(raw)
        if d:
            lo = int(d.group(1)); hi = int(d.group(2)) if d.group(2) else lo
            out["volume"] = (min(lo, hi), max(lo, hi))
    w = _RE_WEEKS.search(raw)
    if w:
        out["weeks"] = int(w.group(1))
    ln = _RE_LENGTH.search(raw)
    if ln:
        lo = int(ln.group(1)); hi = int(ln.group(2)) if ln.group(2) else lo
        out["length"] = (min(lo, hi), max(lo, hi))
    return out


# ---------------------------------------------------------------------------
# Vótery osi — każdy zwraca 'for' / 'against' / 'abstain'
# ---------------------------------------------------------------------------

def vote_params(subject: dict[str, Any], sample: dict[str, Any]) -> str:
    """Parametry zgodne => 'for'; sprzeczne (wspólny klucz, różna wartość) => 'against';
    brak parametrów po którejś stronie => 'abstain'."""
    p_subj = extract_params(subject.get("service_name"))
    p_samp = extract_params(sample.get("service_name"))
    if not p_subj or not p_samp:
        return "abstain"
    common = set(p_subj) & set(p_samp)
    if not common:
        return "abstain"  # różne typy parametrów — nie porównywalne
    # jeśli którykolwiek wspólny parametr się różni => inna usługa.
    # Parametry zakresowe (volume/length: krotki (lo,hi)) porównujemy przez
    # PRZECIĘCIE zakresów — "3D" i "3/4D" to zgodność, "1:1" i "2:1" konflikt.
    for k in common:
        a, b = p_subj[k], p_samp[k]
        if k in ("volume", "length"):
            if a[1] < b[0] or b[1] < a[0]:  # zakresy rozłączne
                return "against"
        elif a != b:
            return "against"
    return "for"  # wszystkie wspólne parametry zgodne


def vote_package(subject: dict[str, Any], sample: dict[str, Any]) -> str:
    """Różny status pakietu => 'against' (pakiet 5 zabiegów to inna usługa niż jeden).
    Zgodny status => 'abstain' (zgodność domyślna, nie dowód tożsamości)."""
    s_pkg = bool(subject.get("is_package", False))
    o_pkg = bool(sample.get("is_package", False))
    return "against" if s_pkg != o_pkg else "abstain"


def _normalize_category(cat: str | None) -> str | None:
    norm = _normalize_text(cat)
    if not norm or norm in ("-", "—", "–"):
        return None
    return norm


def _category_tokens_overlap(a: str, b: str) -> float:
    ta, tb = set(a.split()), set(b.split())
    if not ta or not tb:
        return 0.0
    overlap = ta & tb
    if not overlap:
        return 0.0
    return len(overlap) / min(len(ta), len(tb))


def vote_category(subject: dict[str, Any], sample: dict[str, Any]) -> str:
    """Kategoria zgodna/bliska => 'for'; obie obecne ale rozłączne => 'against';
    pusta LUB neutralna => 'abstain'.

    KLUCZOWE: kategoria NEUTRALNA (usługowo jak "Konsultacje" / eventowo jak
    "Promocje") nie wskazuje domeny, więc nie dowodzi ani tożsamości ani obcości
    — wstrzymuje się, tak samo jak pusta. Inaczej "Konsultacje" dawałoby fałszywe
    odrzucenie tożsamej konsultacji (false-reject z demo prod 2026-06-22).
    """
    if is_neutral_category(subject.get("category_name")) or is_neutral_category(sample.get("category_name")):
        return "abstain"
    c_subj = _normalize_category(subject.get("category_name"))
    c_samp = _normalize_category(sample.get("category_name"))
    if c_subj is None or c_samp is None:
        return "abstain"
    overlap = _category_tokens_overlap(c_subj, c_samp)
    if overlap >= 0.5:
        return "for"
    return "against"  # obie obecne, domenowe, brak wspólnego tokenu => różne


def vote_duration(subject: dict[str, Any], sample: dict[str, Any]) -> str:
    """Tylko SKRAJNA różnica czasu (>=3×) => 'against'; inaczej 'abstain'.
    Czas nie potwierdza tożsamości (ten sam zabieg bywa różnie długi)."""
    d_subj = subject.get("duration_minutes")
    d_samp = sample.get("duration_minutes")
    if not d_subj or not d_samp:
        return "abstain"
    hi, lo = max(d_subj, d_samp), min(d_subj, d_samp)
    if lo > 0 and hi / lo >= _DURATION_RATIO_AGAINST:
        return "against"
    return "abstain"


def _price_per_min(svc: dict[str, Any]) -> float | None:
    """zł/min (w groszach/min) gdy cena i czas dostępne, inaczej None."""
    p = svc.get("price_grosze")
    d = svc.get("duration_minutes")
    if not p or p <= 0:
        return None
    if d and d > 0:
        return p / d
    return None


def vote_price(subject: dict[str, Any], sample: dict[str, Any]) -> str:
    """Rozrzut ceny rzędu wielkości => 'against' (różny zabieg pod zgodną nazwą).

    NIGDY 'for' — cena nie buduje klastra (jest wynikiem), tylko skrajny rozjazd
    przeczy tożsamości. Porównanie per zł/min; gdy brak czasu po którejś stronie,
    fallback do surowych cen. Brak ceny => 'abstain'.
    """
    s_ppm = _price_per_min(subject)
    o_ppm = _price_per_min(sample)
    if s_ppm is not None and o_ppm is not None:
        hi, lo = max(s_ppm, o_ppm), min(s_ppm, o_ppm)
    else:
        sp, op = subject.get("price_grosze"), sample.get("price_grosze")
        if not sp or not op or sp <= 0 or op <= 0:
            return "abstain"
        hi, lo = max(sp, op), min(sp, op)
    if lo > 0 and hi / lo >= PRICE_RATIO_AGAINST:
        return "against"
    return "abstain"


def vote_body_area(subject: dict[str, Any], sample: dict[str, Any]) -> str:
    """Różny ZAKRES obszarów ciała => 'against' (inna usługa). 'abstain' gdy
    zestawy równe lub którykolwiek pusty (nazwa nie wymienia obszaru).

    Wymaga RÓWNOŚCI zestawu, nie overlapu: subject {twarz} vs bliźniak
    {twarz, szyja, dekolt} to różny zakres = inna usługa (oczyszczanie twarzy
    ≠ oczyszczanie twarz+szyja+dekolt), mimo wspólnej 'twarz'. Równe zestawy NIE
    dają 'for' — ten sam obszar nie dowodzi tożsamości (oczyszczanie twarzy ≠
    botoks twarzy, oba {twarz})."""
    s_areas = extract_body_areas(subject.get("service_name") or "")
    o_areas = extract_body_areas(sample.get("service_name") or "")
    if not s_areas or not o_areas:
        return "abstain"  # generyczny zakres — nie wiadomo
    if s_areas == o_areas:
        return "abstain"  # ten sam zakres — nie potwierdza tożsamości
    return "against"      # różny zestaw obszarów = inna usługa


def identity_votes(subject: dict[str, Any], sample: dict[str, Any]) -> dict[str, str]:
    """Zbierz głosy wszystkich osi dla pary (subject, sample)."""
    return {
        "params": vote_params(subject, sample),
        "package": vote_package(subject, sample),
        "body_area": vote_body_area(subject, sample),
        "price": vote_price(subject, sample),
        "category": vote_category(subject, sample),
        "duration": vote_duration(subject, sample),
    }


def identity_margin(votes: dict[str, str], category_weight: float | None = None) -> float:
    """Ważony margines tożsamości: Σ(for·waga) − Σ(against·waga). Abstain = 0.

    Dodatni => osie przeważają ZA tożsamością; ujemny => przeciw.
    category_weight nadpisuje wagę osi kategoria (dynamiczna wg generyczności
    nazwy subjectu — patrz _category_weight_for). None => statyczny fallback.
    """
    cat_w = AXIS_WEIGHTS["category"] if category_weight is None else category_weight
    score = 0.0
    for axis, vote in votes.items():
        w = cat_w if axis == "category" else AXIS_WEIGHTS.get(axis, 0.0)
        if vote == "for":
            score += w
        elif vote == "against":
            score -= w
    return score


def _cutoff_for_strictness(strictness: float) -> float:
    """Próg marginesu zależny od surowości.

    strictness 0.0 => cutoff -1.5: liberalny — odrzuca tylko silne 'against'
                      (np. sprzeczne parametry, lub kilka osi przeciw).
    strictness 1.0 => cutoff +0.1: surowy — wymaga przewagi 'for', każde
                      niezbalansowane 'against' wycina.
    Liniowo pomiędzy. Bliźniak zostaje gdy identity_margin >= cutoff.
    """
    s = max(0.0, min(1.0, float(strictness)))
    return -1.5 + s * 1.6


# Osie STRUKTURALNE — sprzeczność na nich to twarde weto (definitywnie inna usługa,
# niezależnie od surowości): różna ilość produktu (ml/liczba), pakiet vs pojedyncza,
# różny zakres obszarów ciała (twarz vs twarz+szyja+dekolt). Cena, kategoria i czas
# to osie MIĘKKIE (głosy ważone, podlegają surowości).
_HARD_VETO_AXES = ("params", "package", "body_area")


def is_identity_match(
    votes: dict[str, str], strictness: float = 0.5, category_weight: float | None = None
) -> bool:
    """Czy bliźniak jest tożsamy przy danej surowości.

    Najpierw twarde weto osi strukturalnych (sprzeczne parametry / różny pakiet =
    inna usługa zawsze). Potem miękkie osie (kategoria, czas) przez margines i surowość.
    """
    for axis in _HARD_VETO_AXES:
        if votes.get(axis) == "against":
            return False
    return identity_margin(votes, category_weight) >= _cutoff_for_strictness(strictness)


# ---------------------------------------------------------------------------
# Filtr klastra + metryki tożsamości
# ---------------------------------------------------------------------------

def _unique_salons(samples: list[dict[str, Any]]) -> int:
    seen: set[Any] = set()
    for s in samples:
        bid = s.get("booksy_id")
        seen.add(bid if bid is not None else ("__own__", s.get("service_id")))
    return len(seen)


# Surowość referencyjna dla metryki czystości — "rdzeń pewności tożsamości".
# Bliźniak liczy się jako czysty, jeśli jest tożsamy przy tej surowości używając
# PEŁNEJ logiki (waga kategorii wg generyczności + twarde weta). Dzięki temu dla
# nazw specyficznych kategoria-against (niespójność właściciela) NIE psuje czystości,
# a dla generycznych obca kategoria — owszem.
_PURITY_REF_STRICTNESS = 0.6


def cluster_identity_purity(subject: dict[str, Any], samples: list[dict[str, Any]]) -> float:
    """Czystość tożsamościowa klastra: udział bliźniaków, którzy są TOŻSAMI z
    subjectem przy surowości referencyjnej (pełna logika — generyczność + weta).

    1.0 = każdy bliźniak to plausibly ta sama usługa; 0.0 = żaden. Liczona
    NIEZALEŻNIE od ceny. Dla nazwy specyficznej różna kategoria właściciela nie
    obniża czystości (embedding nazwy niesie tożsamość); dla generycznej — obniża.
    Pusty klaster => 1.0 (wystarczalność oceniana osobno).
    """
    if not samples:
        return 1.0
    cat_w = _category_weight_for(subject)
    clean = sum(
        1 for s in samples
        if is_identity_match(identity_votes(subject, s), _PURITY_REF_STRICTNESS, cat_w)
    )
    return clean / len(samples)


def apply_identity_test(
    subject: dict[str, Any],
    samples: list[dict[str, Any]],
    strictness: float = 0.5,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Odsiej z klastra bliźniaki NIE-tożsame przy danej surowości.

    Zwraca (kept, meta). Każdy zachowany bliźniak dostaje pole
    "identity_margin" (do debugowania/sortu). meta zawiera rozkład głosów,
    czystość przed/po i liczbę unikalnych salonów po filtrze. Nie mutuje wejścia.
    """
    cat_w = _category_weight_for(subject)
    kept: list[dict[str, Any]] = []
    against_axes: dict[str, int] = {ax: 0 for ax in AXIS_WEIGHTS}
    for s in samples:
        votes = identity_votes(subject, s)
        for ax, v in votes.items():
            if v == "against":
                against_axes[ax] += 1
        if is_identity_match(votes, strictness, cat_w):
            kept.append({**s, "identity_margin": round(identity_margin(votes, cat_w), 3)})

    meta = {
        "strictness": max(0.0, min(1.0, float(strictness))),
        "category_weight": cat_w,
        "subject_generic": is_generic_name(subject.get("service_name") or ""),
        "n_in": len(samples),
        "n_kept": len(kept),
        "n_dropped": len(samples) - len(kept),
        "n_unique_salons_kept": _unique_salons(kept),
        "against_by_axis": against_axes,
        "purity_in": round(cluster_identity_purity(subject, samples), 3),
        "purity_kept": round(cluster_identity_purity(subject, kept), 3),
    }
    return kept, meta


def adaptive_identity_filter(
    subject: dict[str, Any],
    samples: list[dict[str, Any]],
    min_salons: int = 3,
    purity_target: float = 0.9,
    prefer: str = "sufficiency",
    strictness_grid: tuple[float, ...] = (0.0, 0.25, 0.5, 0.75, 1.0),
) -> tuple[list[dict[str, Any]], float, dict[str, Any]]:
    """DRUGIE PRAWO — dobierz surowość per usługa wg WPŁYWU na tożsamość klastra.

    Idea: nie ma globalnej surowości. Dla danej usługi szukamy NAJMNIEJSZEJ
    surowości, która czyni klaster wystarczająco TOŻSAMYM (czystość >= purity_target),
    zachowując dość niezależnych salonów (>= min_salons). Eskalujemy surowość tylko
    gdy niższa daje wymieszany klaster; nigdy nie eskalujemy ponad to, co potrzebne.

    "Zły wpływ" wykrywany operacyjnie: jeśli wyższa surowość redukuje salony, ale
    NIE podnosi czystości tożsamości — to ślepe cięcie (pewnie po pustych kategoriach
    lub tożsamych), więc go nie wybieramy. Jeśli czysty klaster jest osiągalny tylko
    kosztem zejścia poniżej min_salons — zwracamy najlepszy kompromis wg `prefer`:
      prefer="sufficiency" — wybierz wariant utrzymujący wystarczalność (cena z lekko
                             wymieszanego klastra > brak ceny), flaguj w meta.
      prefer="purity"      — wybierz najczystszy nawet jeśli traci wystarczalność
                             (raczej "za mało danych" niż cena z wymieszanego).

    Zwraca (kept, chosen_strictness, meta). meta["trace"] ma per-surowość
    (n_salons, purity) — surowiec do zestawień i debugowania.
    """
    trace: list[dict[str, Any]] = []
    evaluated: list[tuple[float, list[dict[str, Any]], dict[str, Any]]] = []
    for st in strictness_grid:
        kept, m = apply_identity_test(subject, samples, st)
        trace.append({
            "strictness": st,
            "n_salons": m["n_unique_salons_kept"],
            "purity": m["purity_kept"],
            "n_kept": m["n_kept"],
        })
        evaluated.append((st, kept, m))

    # Kandydaci spełniający OBA progi: czystość i wystarczalność.
    good = [
        (st, kept, m) for (st, kept, m) in evaluated
        if m["purity_kept"] >= purity_target and m["n_unique_salons_kept"] >= min_salons
    ]

    chosen_reason: str
    if good:
        # najmniejsza surowość, która wystarcza (nie psuj więcej niż trzeba)
        st, kept, m = min(good, key=lambda x: x[0])
        chosen_reason = "min_strictness_meeting_purity_and_sufficiency"
    else:
        # żadna surowość nie daje czysto+wystarczalnie — kompromis wg prefer
        if prefer == "purity":
            # najczystszy; remis => więcej salonów
            st, kept, m = max(evaluated, key=lambda x: (x[2]["purity_kept"], x[2]["n_unique_salons_kept"]))
            chosen_reason = "max_purity_no_sufficient_clean"
        else:
            # broni wystarczalności: spośród wariantów z max salonami wybierz najczystszy.
            max_salons = max(m["n_unique_salons_kept"] for _, _, m in evaluated)
            cands = [(st, kept, m) for st, kept, m in evaluated if m["n_unique_salons_kept"] == max_salons]
            st, kept, m = max(cands, key=lambda x: x[2]["purity_kept"])
            chosen_reason = "max_sufficiency_then_purity"

    meta = {
        "chosen_strictness": st,
        "chosen_reason": chosen_reason,
        "purity_target": purity_target,
        "min_salons": min_salons,
        "prefer": prefer,
        "final": m,
        "trace": trace,
    }
    return kept, st, meta
