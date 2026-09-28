"""Rozbiór branży: gdzie taksonomia v9 mówi „ta sama”, a sędzia nie (i odwrotnie).
Czyta pary z ocenami sędziego i destylacje v9; szczegóły sędziego (pytania tak/nie) z pamięci ocen — bez nowych wywołań."""
import argparse, asyncio, json, os, re, sys
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import warianty_destylacji as wd  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from services.typesafe_drzewo.podzial import jako_schemat, porownaj_v8  # noqa: E402
from services.typesafe_ocena.pamiec import OcenaPar  # noqa: E402
from services.typesafe_ocena.pytania import MODEL, para_klucz  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient  # noqa: E402


POZIOM = re.compile(r"podstaw|specjalist|zaawans|lecznicz|komplek|częściow|czesciow|całościow|calosciow|całego ciała|mini\b|maxi|premium|lux|express|ekspres|pełn|rozszerz|standard|vip|exclusive|deluxe|intensywn", re.I)
CZAS = re.compile(r"(\d+)\s*(?:min|minut|h\b|godz)", re.I)
LICZBA = re.compile(r"(\d+)\s*(?:pal|paz|stref|okolic|szt|zabieg|x\b|sesj)", re.I)


def klasy(a: str, b: str, ca, cb) -> list[str]:
    out = []
    if set(m.lower() for m in POZIOM.findall(a)) != set(m.lower() for m in POZIOM.findall(b)):
        out.append("poziom/zakres w nazwie")
    if CZAS.findall(a) != CZAS.findall(b):
        out.append("czas w nazwie")
    if LICZBA.findall(a) != LICZBA.findall(b):
        out.append("liczba w nazwie")
    if ca and cb and max(ca, cb) / max(min(ca, cb), 1) >= 1.25:
        out.append("inny czas w Booksy (≥1,25×)")
    if bool(re.search(r"\+|\bz\b|\boraz\b", a)) != bool(re.search(r"\+|\bz\b|\boraz\b", b)):
        out.append("dodatek/zestaw po jednej stronie")
    return out or ["inne"]


async def main(a):
    D = Path(a.dane)
    pary = json.loads((D / "pary.json").read_text(encoding="utf-8"))
    pam = json.loads((D / a.destylacje).read_text(encoding="utf-8"))
    pod = json.loads((wd.sk.DANE / "2026-09-25" / "podzial_v9.json").read_text(encoding="utf-8"))
    sch = jako_schemat(pod)
    z = [p for p in pary if p["branza"] in a.branze]
    for p in z:
        p["tax"], p["powod"] = porownaj_v8(pam.get(p["ka"]), pam.get(p["kb"]), pod, {"nazwa": p["a"]["nazwa"], "duration_minutes": p.get("czas_a")},
                                          {"nazwa": p["b"]["nazwa"], "duration_minutes": p.get("czas_b")}, _sch=sch)
    key = (os.environ.get("TYPESAFE_API_KEY") or wd.ot.KEY_FILE.read_text(encoding="utf-8")).strip()
    async with AsyncTypeSafeClient(api_key=key, model=MODEL) as client:
        oc = await OcenaPar(SupabaseService().client, client, budzet_usd=0.05).ocen([(p["a"], p["b"]) for p in z])
    for br in a.branze:
        zb = [p for p in z if p["branza"] == br]
        fp = [p for p in zb if p["tax"] == "tozsame" and p["sedzia"] != "tozsame"]
        fn = [p for p in zb if p["tax"] != "tozsame" and p["sedzia"] == "tozsame"]
        print(f"\n=== {br}: par {len(zb)} | sędzia {dict(Counter(p['sedzia'] for p in zb))} | taksonomia {dict(Counter(p['tax'] for p in zb))}")
        print(f"  „ta sama” wg taksonomii: {sum(p['tax']=='tozsame' for p in zb)}, z tego sędzia NIE: {len(fp)} | sędzia „ta sama”, taksonomia NIE: {len(fn)}")
        rodz = Counter((pam.get(p["ka"], {}).get("rodzaj"), pam.get(p["kb"], {}).get("rodzaj")) for p in fp)
        print("  rodzaje w błędnych „ta sama”:", dict(rodz.most_common(8)))
        rozn = Counter()
        for p in fp:
            c = (oc.get(para_klucz(p["a"], p["b"])) or {}).get("cechy") or {}
            rozn.update(k for k, v in c.items() if v is not None and v < 0.5)
        print("  czym się różnią wg sędziego (tak/nie < 0,5):", dict(rozn))
        kl = Counter(k for p in fp for k in klasy(p["a"]["nazwa"], p["b"]["nazwa"], p.get("czas_a"), p.get("czas_b")))
        print(f"  klasy błędnych „ta sama” (para może mieć kilka) na {len(fp)}:", dict(kl.most_common()))
        print("  salonów w próbie:", len({p['salon'] for p in zb}), "| usług podmiotu z błędem:", len({p['ka'] for p in fp}))
        for p in fp[: a.przykladow]:
            ta, tb = pam.get(p["ka"], {}), pam.get(p["kb"], {})
            ca = ta.get("cechy", {}).get(ta.get("rodzaj"), {})
            cb = tb.get("cechy", {}).get(tb.get("rodzaj"), {})
            c = (oc.get(para_klucz(p["a"], p["b"])) or {}).get("cechy") or {}
            print(f"   • {p['a']['nazwa'][:40]!r} [{p['a']['kategoria_w_cenniku'][:22]}] ↔ {p['b']['nazwa'][:40]!r} [{p['b']['kategoria_w_cenniku'][:22]}] "
                  f"| {ta.get('rodzaj')} | {ca} / {cb} | sędzia {p['sedzia']} (zabieg {c.get('ten_sam_zabieg')}, obszar {c.get('ten_sam_obszar')}, wariant {c.get('ten_sam_wariant')})")
        if fn:
            print("  sędzia „ta sama”, taksonomia nie — powody:", dict(Counter(p["powod"].split(",")[0] for p in fn).most_common(6)))


p = argparse.ArgumentParser()
p.add_argument("--dane", default=str(wd.OUT))
p.add_argument("--destylacje", default="destylacje_v9.json")
p.add_argument("--branze", nargs="+", default=["Podologia", "Masaż"])
p.add_argument("--przykladow", type=int, default=22)
asyncio.run(main(p.parse_args()))
