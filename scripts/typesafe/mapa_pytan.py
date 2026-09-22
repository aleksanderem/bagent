"""Mapa: które pytania badania mają sens w której branży.

Po co: zestaw ~50 pytań był wspólny dla wszystkich branż, więc np. medycynę
estetyczną pytaliśmy o sierść zwierzęcia, a barbera o pępek. ~95% tokenów to
pytania, nie nazwa — każde zbędne pytanie to czysty koszt.

Jak: dla każdej grupy pytań liczymy, jaki odsetek zdestylowanych nazw usług
w branży (service_taxonomy.name_sample) w ogóle o tym wspomina. Wykrywanie
jest celowo NADGORLIWE (szerokie rdzenie słów) — zostawienie zbędnego pytania
kosztuje grosze, wycięcie potrzebnego może zepsuć weto.

Próg: grupa zostaje, gdy wspomina o niej ≥ 1 na 1000 nazw w branży. Powód z
natury danych: raport ma ~100–300 usług podmiotu, więc rzadsza cecha dotyczy
średnio < 0,3 usługi na raport. NIE strojone na holdoucie.

Pytania bazowe (odbiorca k/m/dzieci, zakres, pakiet, dodatek, metoda) zostają
zawsze — dotyczą każdej branży.

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/mapa_pytan.py <nazwy_branz.tsv>
  → zapisuje services/typesafe_profile/mapa_pytan.json (plik czytany przez produkcję)
"""

from __future__ import annotations

import csv
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

PROG = 0.001

ZAWSZE = [
    "odb:kobiety", "odb:mezczyzni", "odb:dzieci",
    "zakres:obecny", "zakres:skala", "pakiet", "dodatek", "metoda",
]

# „szczeni” bez początku słowa łapało „zagęszczenie”, „oczyszczenie”, „rozpuszczenie”
# (0,22% nazw medycyny estetycznej) i ratowało pytanie o zwierzęta.
ZWIERZE = r"\bpies|\bpsa\b|\bpsy\b|\bpsow|\bkot(a|y|ow|ek)?\b|zwierz|\bszczeni|siersc|\brasa\b|\byork|maltan|groom"

# grupa: (wzorzec na znormalizowanej nazwie, klucze pytań w grupie)
GRUPY: dict[str, tuple[str, list[str]]] = {
    "dlugosc": (r"krotk|sredni|dlug|ramion|lopat|do pasa|\bcm\b|\bmm\b|\d\s*-\s*\d", ["dl:obecna", "dl:skala"]),
    "objetosc_rzes": (r"\d\s*:\s*\d|\b\d{1,2}\s*d\b|volume|objet|wolum|mega", ["obj:obecna", "obj:skala"]),
    "para": (r"dwoj|dla par|\bpara\b|we dwoje|2 osob|dla 2", ["odb:para"]),
    "ciaza": (r"ciaz|ciez", ["odb:ciaza"]),
    "zwierze": (ZWIERZE, ["odb:zwierze", "ob:siersc_zwierzecia"]),
    "etap:zalozenie": (r"zalozen|przedluz|\bnowe|pierwsz|aplikac", ["etap:zalozenie"]),
    "etap:uzupelnienie": (r"uzupel|dopeln|odrost|refill", ["etap:uzupelnienie"]),
    "etap:zdjecie": (r"zdjec|sciagn|usuniec|zdejm", ["etap:zdjecie"]),
    "etap:korekta": (r"korekt|popraw|napraw", ["etap:korekta"]),
    "etap:konsultacja": (r"konsult|kwalifik", ["etap:konsultacja"]),
    "ob:wlosy": (r"wlos|strzyz|fryz|kolor|farb|balej|ombre|keraty|modelow", ["ob:wlosy"]),
    "ob:boki_glowy": (r"\bboki|bokow|fade|podgol|tyl glowy", ["ob:boki_glowy"]),
    "ob:skora_glowy": (r"skor\w* glowy|trych|skalp|head spa|glowy", ["ob:skora_glowy"]),
    "ob:broda": (r"brod|zarost|\bwas|barber", ["ob:broda"]),
    "ob:skora_twarzy": (r"twarz|\bcer[ay]", ["ob:skora_twarzy"]),
    "ob:okolice_oczu": (r"oczu|\boko\b|powiek|oczy", ["ob:okolice_oczu"]),
    "ob:usta": (r"\busta?\b|warg", ["ob:usta"]),
    "ob:nos": (r"\bnos", ["ob:nos"]),
    "ob:uszy": (r"\bucho|\bucha|\buszu|\buszy|platk|helix|chrzastk", ["ob:uszy"]),
    "ob:szyja": (r"szyj|kark", ["ob:szyja"]),
    "ob:dekolt": (r"dekolt", ["ob:dekolt"]),
    "ob:brwi": (r"brwi|\bbrew", ["ob:brwi"]),
    "ob:rzesy": (r"rzes", ["ob:rzesy"]),
    "ob:plecy": (r"plec|plecy", ["ob:plecy"]),
    "ob:brzuch": (r"brzuch|boczk", ["ob:brzuch"]),
    "ob:rece": (r"\brece|\brak\b|ramion|przedrami", ["ob:rece"]),
    "ob:dlonie": (r"dlon", ["ob:dlonie"]),
    "ob:paznokcie_dloni": (r"manicure|paznok|hybryd|\bzel|tips|akryl", ["ob:paznokcie_dloni"]),
    "ob:paznokcie_stop": (r"pedicure|paznok\w* stop", ["ob:paznokcie_stop"]),
    "ob:stopy": (r"\bstop|piet|pedicure|podolog", ["ob:stopy"]),
    "ob:nogi": (r"\bnog|\buda\b|\budo\b|lydk|kolan", ["ob:nogi"]),
    "ob:posladki": (r"poslad", ["ob:posladki"]),
    "ob:pachy": (r"\bpach", ["ob:pachy"]),
    "ob:bikini": (r"bikini|intym|pachwin|brazylij", ["ob:bikini"]),
    "ob:cale_cialo": (r"cial|calosc", ["ob:cale_cialo"]),
    "ob:jezyk": (r"jezyk", ["ob:jezyk"]),
    "ob:pepek": (r"pepk|pepek", ["ob:pepek"]),
    "ob:sutki": (r"sutk|sutek", ["ob:sutki"]),
    "ob:zeby": (r"\bzeb|tooth|gems", ["ob:zeby"]),
}


def norm(s: str) -> str:
    t = (s or "").lower().replace("ł", "l")
    t = unicodedata.normalize("NFKD", t)
    return "".join(c for c in t if not unicodedata.combining(c))


def build(tsv: Path) -> tuple[dict[str, list[str]], dict[str, dict[str, float]]]:
    names: dict[str, list[str]] = defaultdict(list)
    with tsv.open(encoding="utf-8", newline="") as f:
        for row in csv.reader(f, delimiter="\t"):
            if len(row) == 2:
                names[row[0]].append(norm(row[1]))
    compiled = {g: re.compile(p) for g, (p, _) in GRUPY.items()}
    share: dict[str, dict[str, float]] = {}
    mapa: dict[str, list[str]] = {}
    for branch, lst in names.items():
        hits = Counter()
        for n in lst:
            for g, rx in compiled.items():
                if rx.search(n):
                    hits[g] += 1
        share[branch] = {g: hits[g] / len(lst) for g in GRUPY}
        keep = list(ZAWSZE)
        for g, (_, keys) in GRUPY.items():
            if share[branch][g] >= PROG:
                keep += keys
        mapa[branch] = sorted(set(keep))
    return mapa, share


def main() -> None:
    """Źródła łączone SUMĄ: grupa zostaje, jeśli którekolwiek źródło ją pokazuje.

    Dwa źródła, bo service_taxonomy trzyma nazwy w branży ZABIEGU, a weto szuka
    po branży SALONU (nazwy_salonow.py) — sama pierwsza mapa wycinała np.
    salonom paznokci pytania o nogi i pachy (depilacja).
    """
    maps, shares = zip(*(build(Path(p)) for p in sys.argv[1:]))
    branches = sorted(set().union(*maps))
    mapa = {b: sorted(set().union(*(m.get(b, ZAWSZE) for m in maps))) for b in branches}
    out = Path(__file__).resolve().parents[2] / "services" / "typesafe_profile" / "mapa_pytan.json"
    out.write_text(json.dumps(mapa, ensure_ascii=False, indent=1), encoding="utf-8")
    for branch in branches:
        dropped = [g for g in GRUPY if all(s.get(branch, {}).get(g, 0) < PROG for s in shares)]
        print(f"{branch:<22} pytań {len(mapa[branch]):>2}  wycięte ({len(dropped)}): {', '.join(dropped)}")
    print(f"zapisano {out}")


if __name__ == "__main__":
    main()
