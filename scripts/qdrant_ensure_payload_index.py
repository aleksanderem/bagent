"""Zakłada (idempotentnie) indeks payload `booksy_id` w kolekcji Qdrant.

Dlaczego: kolekcja ma payload_schema={} — filtrowany HNSW po booksy_id bez
indeksu jest przybliżony i dla małej puli (15 wybranych konkurentów) gubi
CAŁE salony (raport 250: 10/15 z 0 bliźniakami, exact=True: 143–174 każdy).
Z indeksem Qdrant szacuje kardynalność filtra i dla małej puli robi dokładny
skan podzbioru. Operacja ADDYTYWNA (nie zmienia wektorów ani payloadu), ale
to zmiana na PROD Qdrant — uruchamiaj świadomie, po potwierdzeniu.

Użycie: PYTHONPATH=. uv run python scripts/qdrant_ensure_payload_index.py [--apply]
Bez --apply tylko pokazuje aktualny payload_schema.
"""
import sys
from dotenv import load_dotenv

load_dotenv()

from qdrant_client import models  # noqa: E402
from services.similarity_pricing.qdrant_search import COLLECTION, get_client  # noqa: E402


def main() -> None:
    qc = get_client()
    info = qc.get_collection(COLLECTION)
    schema = {k: str(v.data_type) for k, v in (info.payload_schema or {}).items()}
    print(f"{COLLECTION}: points={info.points_count} payload_schema={schema}")
    if "booksy_id" in schema:
        print("indeks booksy_id już istnieje — nic do zrobienia")
        return
    if "--apply" not in sys.argv:
        print("brak indeksu booksy_id; dodaj --apply, żeby go założyć")
        return
    res = qc.create_payload_index(
        COLLECTION, field_name="booksy_id",
        field_schema=models.PayloadSchemaType.INTEGER, wait=True,
    )
    print("create_payload_index:", res)
    info = qc.get_collection(COLLECTION)
    print("payload_schema po:", {k: str(v.data_type) for k, v in (info.payload_schema or {}).items()})


if __name__ == "__main__":
    main()
