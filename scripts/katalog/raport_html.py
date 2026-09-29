"""Katalog usług — strona HTML testowego raportu konkurencji z raport.json (raport_testowy.py --licz).

Użycie: bagent/.venv/bin/python bagent/scripts/katalog/raport_html.py <katalog raportu> <plik.html>
Opcjonalnie w katalogu: ocena_claude.json — moja ocena dopasowań „ta sama” ({"<id oferty>|<id konkurencji>": "T"|"P"|"I"}).
"""
from __future__ import annotations

import html
import json
import sys
from collections import defaultdict
from pathlib import Path

STYL = """
:root {
  /* Jeden jasny wygląd jak produkt (DESIGN.md: wyłącznie light mode). Złoto tylko na tym, co trzeba znaleźć. */
  --bg: #FDF8F2; --surface: #FFFFFF; --ink: #18202B; --muted: #5B6676; --line: #E6DED3;
  --brand: #D4A574; --brand-ink: #8F5528; --eyebrow: #9A6A3A; --tint: #FBF1E4;
  --good: #047857; --good-tint: #E7F6EF; --warn: #92400E; --warn-tint: #FDF3E1; --bad: #9F1239; --bad-tint: #FCE8EC;
  --font: "Outfit", system-ui, -apple-system, "Segoe UI", sans-serif;
  color-scheme: light;
}
* { box-sizing: border-box; }
body { background: var(--bg); color: var(--ink); font-family: var(--font); font-size: 15px; line-height: 1.5; }
.wrap { max-width: 1040px; margin: 0 auto; padding-inline: 16px; padding-block: 28px 56px; display: grid; gap: 28px; }
.eyebrow { font-size: 11px; font-weight: 700; letter-spacing: .16em; text-transform: uppercase; color: var(--eyebrow); }
h1 { font-size: clamp(26px, 4vw, 34px); font-weight: 700; line-height: 1.15; margin: 6px 0 8px; text-wrap: balance; }
h2 { font-size: 20px; font-weight: 600; margin: 0 0 10px; text-wrap: balance; }
p { margin: 0; max-width: 68ch; }
.muted { color: var(--muted); }
.head { display: grid; gap: 6px; }
.meta { display: flex; flex-wrap: wrap; gap: 8px 18px; color: var(--muted); font-size: 14px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; }
.stat { background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 14px 16px; display: grid; gap: 2px; }
.stat b { font-size: 28px; font-weight: 700; font-variant-numeric: tabular-nums; }
.stat.key { border-color: var(--brand); background: var(--tint); }
.stat span { color: var(--muted); font-size: 13px; }
.note { background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 16px 18px; display: grid; gap: 8px; }
.note ul { margin: 0; padding-left: 18px; display: grid; gap: 4px; }
.tbl { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 12px; }
table { border-collapse: collapse; width: 100%; min-width: 720px; font-variant-numeric: tabular-nums; }
th { text-align: left; font-size: 11px; font-weight: 700; letter-spacing: .12em; text-transform: uppercase; color: var(--eyebrow);
     padding: 12px 14px; border-bottom: 1px solid var(--line); background: var(--tint); }
td { padding: 10px 14px; border-bottom: 1px solid var(--line); vertical-align: top; }
tr.grp td { background: var(--bg); font-weight: 600; font-size: 13px; color: var(--muted); padding-block: 8px; }
.num { text-align: right; white-space: nowrap; }
.name { font-weight: 600; }
.var { color: var(--muted); font-size: 13px; }
.pill { display: inline-block; border-radius: 999px; padding: 2px 9px; font-size: 11px; font-weight: 700; letter-spacing: .08em;
        text-transform: uppercase; white-space: nowrap; }
.pill.same { background: var(--good-tint); color: var(--good); }
.pill.sim { background: var(--warn-tint); color: var(--warn); }
.pill.none { background: var(--bg); color: var(--muted); border: 1px solid var(--line); }
.up { color: var(--bad); font-weight: 600; } .down { color: var(--good); font-weight: 600; }
details { margin-top: 6px; } summary { cursor: pointer; color: var(--brand-ink); font-size: 13px; }
summary:focus-visible { outline: 2px solid var(--brand-ink); outline-offset: 2px; border-radius: 4px; }
.m { display: grid; grid-template-columns: minmax(0, 1.2fr) minmax(0, 2fr) auto; gap: 2px 12px; font-size: 13px; margin-top: 6px; }
.m .p { text-align: right; white-space: nowrap; }
.ok { color: var(--good); } .bad { color: var(--bad); }
footer { color: var(--muted); font-size: 13px; display: grid; gap: 4px; }
"""


def odm(n: int, jeden: str, kilka: str, wiele: str) -> str:
    """Polska odmiana liczebnika: 1 salon, 2–4 salony, 5+ salonów (12–14 → wiele)."""
    if n == 1:
        return f"{n} {jeden}"
    return f"{n} {kilka if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else wiele}"


def zl(x: float | None) -> str:
    return "—" if x is None else f"{x:,.0f} zł".replace(",", " ")


def roznica(cena: float | None, rynek: float | None) -> str:
    if not cena or not rynek:
        return "—"
    d = (cena - rynek) / rynek * 100
    klasa = "up" if d > 8 else "down" if d < -8 else ""
    return f'<span class="{klasa}">{d:+.0f}%</span>'


def dopasowania(lista: list[dict], ocena: dict[str, str], sid: str) -> str:
    if not lista:
        return ""
    wiersze = []
    for m in sorted(lista, key=lambda m: (m["salon"], m["cena"])):
        o = ocena.get(f"{sid}|{m.get('id_oferty', '')}")
        znak = f' <b class="{"ok" if o == "T" else "bad"}">{"✓" if o == "T" else "✗ " + o}</b>' if o else ""
        nazwa = html.escape(m["nazwa"]) + (f' <span class="var">· {html.escape(m["wariant"])}</span>' if m.get("wariant") else "")
        wiersze.append(f'<span>{html.escape(m["salon"])}</span><span>{nazwa}{znak}</span><span class="p">{zl(m["cena"])}</span>')
    salony = len({m["booksy_id"] for m in lista})
    return (f'<details><summary>{odm(len(lista), "oferta", "oferty", "ofert")} z {odm(salony, "salonu", "salonów", "salonów")}</summary>'
            f'<div class="m">{"".join(wiersze)}</div></details>')


def main() -> None:
    kat, cel = Path(sys.argv[1]), Path(sys.argv[2])
    tytul = sys.argv[3] if len(sys.argv) > 3 else None
    r = json.loads((kat / "raport.json").read_text(encoding="utf-8"))
    ocena = json.loads((kat / "ocena_claude.json").read_text(encoding="utf-8")) if (kat / "ocena_claude.json").exists() else {}
    s, w = r["salon"], r["wiersze"]
    p = s["podmiot"]
    ta_sama = [x for x in w if x["ta_sama_stat"]["n"] >= 3]
    podobne = [x for x in w if x["ta_sama_stat"]["n"] < 3 and x["podobne_stat"]["n"] >= 3]
    grupy = defaultdict(list)
    for x in w:
        grupy[x["kategoria"] or "Bez kategorii"].append(x)
    rows = []
    for g, lst in grupy.items():
        rows.append(f'<tr class="grp"><td colspan="5">{html.escape(g)}</td></tr>')
        for x in lst:
            st_t, st_p = x["ta_sama_stat"], x["podobne_stat"]
            if st_t["n"] >= 3:
                rynek, znak, st, lst_m = st_t.get("rynkowa"), '<span class="pill same">ta sama</span>', st_t, x["ta_sama"]
            elif st_p["n"] >= 3:
                rynek, znak, st, lst_m = st_p.get("rynkowa"), '<span class="pill sim">podobne</span>', st_p, x["podobne"]
            else:
                rynek, znak, st, lst_m = None, f'<span class="pill none">za mało ({st_t["n"]} / {st_p["n"]})</span>', None, x["ta_sama"] or x["podobne"]
            zakres = f'<div class="var">{zl(st["p25"])}–{zl(st["p75"])} · {odm(st["n"], "salon", "salony", "salonów")}</div>' if st else ""
            nazwa = html.escape(x["nazwa"]) + (f'<div class="var">{html.escape(x["wariant"])}</div>' if x["wariant"] else "")
            czas = f'{x["min"]:.0f} min' if x.get("min") else "—"
            rows.append(f'<tr><td><div class="name">{nazwa}</div>{dopasowania(lst_m, ocena, x["id"])}</td>'
                        f'<td class="num">{zl(x["cena"])}<div class="var">{czas}</div></td>'
                        f'<td>{znak}</td><td class="num">{zl(rynek)}{zakres}</td><td class="num">{roznica(x["cena"], rynek)}</td></tr>')
    # tylko pary, które TERAZ są „ta sama” — pamięć ocen trzyma też pary z poprzednich wersji dopasowania
    biezace = [f'{x["id"]}|{m["id_oferty"]}' for x in w for m in x["ta_sama"]]
    ocen = [ocena[k] for k in biezace if k in ocena]
    bez_oceny = len(biezace) - len(ocen)
    jakosc = (f'<p>Sprawdziłem ręcznie {len(ocen)} par „ta sama” według modelu tej samej usługi: trafne '
              f'{sum(v == "T" for v in ocen)} ({sum(v == "T" for v in ocen) / len(ocen):.0%})'
              + (f'; {bez_oceny} bez oceny' if bez_oceny else '') + '. Znaczek przy ofercie: '
              '✓ ta sama, ✗ P podobna, ✗ I inna.</p>') if ocen else ""
    strona = f"""<title>{html.escape(tytul or "Test dopasowania " + p['name'][:40])}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&display=swap">
<style>{STYL}</style>
<main class="wrap">
  <header class="head">
    <div class="eyebrow">Raport konkurencji · test nowego dopasowania usług</div>
    <h1>{html.escape(p['name'])}</h1>
    <div class="meta"><span>{html.escape(p['typ'])}</span><span>{html.escape(p['city'] or '')}</span>
      <span>{odm(p['uslug'], 'usługa', 'usługi', 'usług')}, {odm(len(w), 'oferta', 'oferty', 'ofert')} z wariantami</span><span>{len(s['konkurenci'])} konkurentów tej branży do {max(k['km'] for k in s['konkurenci']):.1f} km</span></div>
  </header>
  <section class="stats">
    <div class="stat key"><b>{len(ta_sama)}</b><span>ofert z ceną tej samej usługi (≥ 3 salony)</span></div>
    <div class="stat"><b>{len(podobne)}</b><span>ofert tylko z cenami podobnych usług</span></div>
    <div class="stat"><b>{len(w) - len(ta_sama) - len(podobne)}</b><span>ofert bez porównania</span></div>
  </section>
  <section class="note">
    <h2>Jak to policzono</h2>
    <ul>
      <li>Cennik salonu i 25 najbliższych studiów tej samej branży z bazy (odczyt), każdy wariant z własną ceną to osobna oferta.</li>
      <li>Cechy każdej oferty wyciągnął model z abonamentu Z.ai; „ta sama” = ten sam zbiór znaczących słów oferty, uzupełniony o to, co mówi nagłówek kategorii cennika (np. „laserowa”, „PREMIUM”).</li>
      <li>Różnicę rozstrzygał TypeSafe raz na rodzaj różnicy: dopisek po jednej stronie (czy „do 15 cm” zmienia usługę) albo inne słowo po każdej stronie (czy „głowy” i „włosów” to to samo); dodatek w nazwie = podobna.</li>
      <li>Cena rynkowa jak w produkcji: mediana ceny za minutę × czas usługi salonu, gdy znany czas; jeden głos na salon.</li>
      <li>Gdy „tej samej” są mniej niż 3 salony, pokazane są ceny podobnych usług (ten sam zabieg i metoda, inny szczegół).</li>
    </ul>
    {jakosc}
  </section>
  <section class="tbl"><table>
    <thead><tr><th>Oferta salonu</th><th class="num">Cena</th><th>Porównanie</th><th class="num">Rynek</th><th class="num">Różnica</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table></section>
  <footer><span>Test poza silnikiem produkcyjnym — nic nie zapisano w bazie. Pula w promieniu 15 km: {s['pula_15km']} salonów.</span>
    <span>Dane: cenniki Booksy z bazy BooksyAudit, stan z dnia pobrania.</span></footer>
</main>
"""
    cel.write_text(strona, encoding="utf-8")
    print(f"zapisano {cel}")


if __name__ == "__main__":
    main()
