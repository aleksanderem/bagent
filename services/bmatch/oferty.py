"""Oferty b-card (tabela bcard_oferta): usługi aktualnego skanu (chain-head) salonu → karta + dane do doboru.

Używane przez jednorazowe zasilenie (scripts/bmatch/zasil.py, karty z pliku b-card) i odświeżanie po nowych
skanach (karty z bazy, brakujące z punktu b-card). Zapis tylko do tabel b-card — nic innego nie jest ruszane.
"""
from __future__ import annotations

from typing import Any

from . import dobor
from .klucz import klucz_uslugi, wiadomosci_bcard

POLA = "id,booksy_id,name,category_name,description,variants,treatment_name,duration_minutes,is_package,is_active"


def wiersz_oferty(u: dict[str, Any], booksy_id: int, klucz: str, karta: dict[str, Any]) -> dict[str, Any]:
    return {"service_id": int(u["id"]), "booksy_id": int(booksy_id), "klucz": klucz, "nazwa": u["name"],
            "zabiegi": dobor.zabiegi(karta), "skladniki": [list(x) for x in dobor.skladniki(karta)],
            "marka": karta.get("marka"), "poza_beauty": dobor.poza_beauty(karta)}


def glowy_skanow(client: Any, booksy_ids: list[int]) -> dict[int, str]:
    """booksy_id → id aktualnego skanu (chain-head)."""
    out: dict[int, str] = {}
    for i in range(0, len(booksy_ids), 500):
        res = (client.table("salon_scrapes").select("id,booksy_id").in_("booksy_id", booksy_ids[i:i + 500])
               .eq("is_chain_head", True).execute())
        for r in res.data or []:
            out[int(r["booksy_id"])] = r["id"]
    return out


def uslugi_skanu(client: Any, scrape_id: str) -> list[dict[str, Any]]:
    out, od = [], 0
    while True:
        r = client.table("salon_scrape_services").select(POLA).eq("scrape_id", scrape_id).range(od, od + 999).execute()
        out += r.data or []
        if len(r.data or []) < 1000:
            return [u for u in out if u.get("is_active", True) and u.get("name")]
        od += 1000


def oferty_salonu(uslugi: list[dict[str, Any]], booksy_id: int, karty: dict[str, dict[str, Any]]
                  ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """→ (wiersze ofert dla usług z kartą, usługi bez karty)."""
    gotowe, brak = [], []
    for u in uslugi:
        k = klucz_uslugi(u)
        if k in karty:
            gotowe.append(wiersz_oferty(u, booksy_id, k, karty[k]))
        else:
            brak.append(u)
    return gotowe, brak


def zapisz(client: Any, booksy_id: int, wiersze: list[dict[str, Any]]) -> None:
    """Upsert ofert salonu i usunięcie ofert, których nie ma już w aktualnym skanie."""
    for i in range(0, len(wiersze), 500):
        client.table("bcard_oferta").upsert(wiersze[i:i + 500], on_conflict="service_id").execute()
    aktualne = [w["service_id"] for w in wiersze]
    q = client.table("bcard_oferta").delete().eq("booksy_id", booksy_id)
    if aktualne:
        q = q.not_.in_("service_id", aktualne)
    q.execute()


def wejscia_bcard(brak: list[dict[str, Any]], typ_salonu: str = "") -> list[list[dict[str, str]]]:
    return [wiadomosci_bcard(u, typ_salonu) for u in brak]
