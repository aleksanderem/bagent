"""Smoke test hybrid RPC: trigram + cosine on Floral problematic names."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from config import settings
from services.sb_client import make_supabase_client


TEST_CASES = [
    ("Pedicure Hybrydowy", 10),
    ("MANICURE MESKI", 10),
    ("MANICURE HYBRYDOWY", 10),
    ("Pedicure clasyczny z odżywką", 10),
    ("Klasyczny z malowanie zwykłym", 10),
    ("Henna + regulacja brwi 💖", 10),  # parent wrong (10) on purpose — Floral mistake
    ("Usunięcie masy hybrydowej + odżywką", 10),
    ("Uwagi + Dodatki !!!", 10),
    ("Pedicure  Meski", 10),
    ("Przedłużanie rzes mega wolume(5D-10D)", 188),
    ("Botoks dla włosów - intensywna regeneracja", 6),
    ("Strzyżenie grzywki", 6),
    ("Zabieg BTX", 6),
    ("Modelowanie", 6),
]


def main() -> int:
    client = make_supabase_client(settings.supabase_url, settings.supabase_service_key)
    oai = OpenAI()

    names = [n for n, _ in TEST_CASES]
    resp = oai.embeddings.create(model="text-embedding-3-small", input=names)
    embeddings = [d.embedding for d in resp.data]

    print(f"{'service name':<48}  {'parent':>6}  {'tid':>4}  {'canon':<30}  {'tri':>5}  {'cos':>5}  {'score':>5}  src")
    print("-" * 140)
    for (name, parent), emb in zip(TEST_CASES, embeddings):
        res = client.rpc(
            "match_treatment_hybrid",
            {
                "p_name": name,
                "p_embedding": emb,
                "p_parent_hint": parent,
                "p_min_score": 0.30,
            },
        ).execute()
        rows = res.data or []
        if not rows:
            print(f"{name[:48]:<48}  {parent:>6}  {'—':>4}  {'(no match)':<30}  {'—':>5}  {'—':>5}  {'—':>5}")
            continue
        r = rows[0]
        print(
            f"{name[:48]:<48}  {parent:>6}  {r['inferred_tid']:>4}  "
            f"{r['canonical_name'][:30]:<30}  {r['trigram_sim']:>5.2f}  {r['cosine_sim']:>5.2f}  "
            f"{r['score']:>5.2f}  {r['source']}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
