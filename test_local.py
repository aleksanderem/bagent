"""Local test: convert booksy JSON to ScrapedData and run a quick MiniMax call."""

import asyncio
import json
import sys

from config import settings
from services.minimax import MiniMaxClient


def convert_booksy_json(path: str) -> dict:
    """Convert raw booksy-extractor JSON to ScrapedData format."""
    with open(path) as f:
        raw = json.load(f)

    biz = raw["business"]
    categories = []
    total_services = 0

    for cat in biz.get("service_categories", []):
        services = []
        for svc in cat.get("services", []):
            # Get price from service or first variant
            price = svc.get("price")
            duration = None
            variants_out = []

            raw_variants = svc.get("variants", [])
            if raw_variants:
                first = raw_variants[0]
                if price is None:
                    price = first.get("price")
                dur_min = first.get("duration")
                if dur_min:
                    duration = f"{dur_min} min"

                # Only output named variants (with label)
                for v in raw_variants:
                    if v.get("label"):
                        v_price = v.get("service_price", str(v.get("price", "")))
                        v_dur = f"{v['duration']} min" if v.get("duration") else None
                        variants_out.append({
                            "label": v["label"],
                            "price": str(v_price) if v_price else "-",
                            "duration": v_dur,
                        })

            price_str = f"{price} zł" if price else "-"
            services.append({
                "name": svc["name"],
                "price": price_str,
                "duration": duration,
                "description": svc.get("description") or None,
                "imageUrl": None,
                "variants": variants_out if variants_out else None,
            })
            total_services += 1

        categories.append({"name": cat["name"], "services": services})

    location = biz.get("location", {})
    address_parts = [location.get("address", ""), location.get("city", "")]
    address = ", ".join(p for p in address_parts if p)

    return {
        "salonName": biz.get("name"),
        "salonAddress": address or None,
        "salonLogoUrl": biz.get("thumbnail_photo"),
        "categories": categories,
        "totalServices": total_services,
    }


async def test_minimax_call(scraped_data: dict) -> None:
    """Test a single generate_json call to verify ThinkingBlock handling."""
    client = MiniMaxClient(settings.minimax_api_key, settings.minimax_base_url, settings.minimax_model)

    # Build a small pricelist
    text = f"SALON: {scraped_data['salonName']}\n\n"
    for cat in scraped_data["categories"][:2]:
        text += f"## {cat['name']}\n"
        for s in cat["services"][:3]:
            text += f"- {s['name']}: {s['price']}\n"

    prompt = f"""Oceń JAKOŚĆ NAZW usług. Skala 0-20. Zwróć TYLKO JSON:

{{"score": <0-20>, "issues": [{{"severity": "minor", "issue": "przykład", "impact": "wpływ", "affectedCount": 1, "example": "x", "fix": "y"}}]}}

{text}"""

    print(f"Sending prompt ({len(prompt)} chars)...")
    try:
        result = await client.generate_json(prompt)
        print(f"SUCCESS: {json.dumps(result, indent=2, ensure_ascii=False)}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")

        # Debug: make raw call to see what MiniMax returns
        print("\n--- Debug: raw response content blocks ---")
        response = await client.client.messages.create(
            model=client.model,
            max_tokens=4096,
            temperature=0.3,
            system="Odpowiadaj WYŁĄCZNIE poprawnym JSON.",
            messages=[{"role": "user", "content": prompt}],
        )
        for i, block in enumerate(response.content):
            print(f"  [{i}] type={block.type}, attrs={[a for a in dir(block) if not a.startswith('_')]}")
            if hasattr(block, "text"):
                print(f"       text={block.text[:200]}")
            if hasattr(block, "thinking"):
                print(f"       thinking={block.thinking[:200]}")


async def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/Users/alex/Desktop/MOJE_PROJEKTY/booksy-extractor/output/json/264462.json"
    scraped = convert_booksy_json(path)
    print(f"Converted: {scraped['salonName']}, {scraped['totalServices']} services, {len(scraped['categories'])} categories")
    print()
    await test_minimax_call(scraped)


if __name__ == "__main__":
    asyncio.run(main())
