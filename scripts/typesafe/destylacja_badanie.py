"""Destylacja-badanie nazw z holdoutu (mig 188) przez SDK TypeSafe.

Dla każdej unikalnej (branża, nazwa) z matching_holdout wysyła JEDNO wywołanie
z kompletem pytań z badanie_uslugi.py i zapisuje profil liczbowy. Tylko odczyt
bazy; niczego nie zapisuje do service_taxonomy. Model przypięty do wersji
(docs/models: alias się przesuwa, a porównujemy dwa przebiegi i progi).

Użycie:
  bagent/.venv/bin/python bagent/scripts/typesafe/destylacja_badanie.py --out <katalog> --przebieg 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

BAGENT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(BAGENT_ROOT)
sys.path.insert(0, str(BAGENT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import badanie_uslugi  # noqa: E402
from services.supabase import SupabaseService  # noqa: E402
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy  # noqa: E402

MODEL = "jev-1.13.0"
USD_PER_M_INPUT = 0.042  # docs.typesafe.ai/models, 2026-09-21
KEY_FILE = Path.home() / ".config" / "typesafe" / "api_key"


def name_key(name: str | None) -> str:
    return " ".join((name or "").lower().split())


def holdout_names() -> list[dict[str, Any]]:
    rows = (
        SupabaseService().client.table("matching_holdout")
        .select("branza,subject_name,subject_category,cand_name,cand_category")
        .execute()
        .data
        or []
    )
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        for name, cat in ((r["subject_name"], r.get("subject_category")), (r["cand_name"], r.get("cand_category"))):
            key = (r["branza"], name_key(name))
            if key[1] and key not in seen:
                seen[key] = {"branza": r["branza"], "name_key": key[1], "nazwa": name, "kategoria": cat}
    return list(seen.values())


def file_names(path: Path) -> list[dict[str, Any]]:
    """Nazwy z pliku par (np. nowa_partia.jsonl) — ten sam kształt co holdout."""
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        for name, cat in ((r["subject_name"], r.get("subject_category")), (r["cand_name"], r.get("cand_category"))):
            key = (r["branza"], name_key(name))
            if key[1] and key not in seen:
                seen[key] = {"branza": r["branza"], "name_key": key[1], "nazwa": name, "kategoria": cat}
    return list(seen.values())


async def run(args: argparse.Namespace) -> None:
    items = (file_names(Path(args.z_pliku)) if args.z_pliku else holdout_names())[: args.limit or None]
    print(f"nazw do zbadania: {len(items)}, model {MODEL}")
    mapa = json.loads(Path(args.mapa).read_text(encoding="utf-8")) if args.mapa else {}
    sem = asyncio.Semaphore(args.concurrency)
    api_key = os.environ.get("TYPESAFE_API_KEY") or KEY_FILE.read_text(encoding="utf-8")
    t0 = time.monotonic()
    async with AsyncTypeSafeClient(api_key=api_key, model=MODEL, retry=RetryPolicy(max_retries=4, timeout=60.0)) as client:

        async def one(it: dict[str, Any]) -> dict[str, Any]:
            state = badanie_uslugi.build_state(it["nazwa"], it["kategoria"], it["branza"])
            allowed = set(mapa[it["branza"]]) if it["branza"] in mapa else None
            questions = badanie_uslugi.build_questions(it["branza"], allowed)
            async with sem:
                try:
                    result = await client.system_one(state, questions)
                except Exception as e:  # noqa: BLE001 — każdy błąd trafia do wyniku
                    return {**it, "blad": f"{type(e).__name__}: {str(e)[:200]}"}
            return {
                **it,
                "pytan": len(questions),
                "profil": badanie_uslugi.profile(result),
                "tokeny": result.usage.input_tokens or 0,
                "model": result.model,
            }

        rows = await asyncio.gather(*[one(it) for it in items])

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"profile_{args.przebieg}.jsonl"
    path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")
    ok = [r for r in rows if "blad" not in r]
    tokens = sum(r["tokeny"] for r in ok)
    print(json.dumps({
        "nazw": len(rows),
        "bledow": len(rows) - len(ok),
        "pytan_na_nazwe_srednio": round(sum(r["pytan"] for r in ok) / max(len(ok), 1), 1),
        "tokeny_wejscia": tokens,
        "koszt_usd": round(tokens * USD_PER_M_INPUT / 1_000_000, 4),
        "sekund": round(time.monotonic() - t0, 1),
        "plik": str(path),
    }, ensure_ascii=False, indent=2))
    for r in rows:
        if "blad" in r:
            print("BŁĄD:", r["nazwa"], r["blad"])
            break


def main() -> None:
    p = argparse.ArgumentParser(description="Destylacja-badanie nazw holdoutu przez SDK TypeSafe")
    p.add_argument("--out", required=True)
    p.add_argument("--przebieg", default="1")
    p.add_argument("--limit", type=int, default=0, help="0 = wszystkie; do próby na kilku nazwach")
    p.add_argument("--concurrency", type=int, default=6)
    p.add_argument("--z-pliku", default=None, help="JSONL par (np. nowa_partia.jsonl) zamiast holdoutu")
    p.add_argument("--mapa", default=None, help="mapa_pytan.json — tylko pytania istotne w branży")
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
