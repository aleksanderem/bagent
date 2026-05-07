"""OpenAI gpt-image-1 wrapper for ad creative generation.

The new gpt-image-1 model (released 2025-03) is markedly better than
DALL-E 3 for branded marketing creative — handles Polish prompts
natively, supports inline text in images (rare for diffusion models),
and produces consistent visual style across variants.

API doc: https://platform.openai.com/docs/api-reference/images/create
Model:   gpt-image-1
Sizes:   1024x1024, 1024x1536 (portrait/IG story), 1536x1024 (landscape)
Quality: low | medium | high   — beauty/medical needs high
Returns: base64 JSON, no URL hosting (we save to Supabase storage)

Cost (verified 2025): high quality 1024x1024 ≈ $0.17 per image, low ≈ $0.04.
For ad creative we use medium ($0.07) — good enough for IG feed, far
cheaper at scale than human designer.

This module covers:
    - generate_creative(prompt, size) → bytes (PNG)
    - overlay_brand(image_bytes, salon_data) → bytes (with logo + price + CTA)
    - generate_ad_variants(salon, service, n=3) → list[bytes]
"""

from __future__ import annotations

import asyncio
import base64
import io
import logging
from dataclasses import dataclass
from typing import Literal

import httpx
from PIL import Image, ImageDraw, ImageFont

from config import settings

logger = logging.getLogger("bagent.services.openai_image")

OpenAISize = Literal["1024x1024", "1024x1536", "1536x1024"]
OpenAIQuality = Literal["low", "medium", "high"]


@dataclass
class CreativeOverlay:
    """Brand overlay parameters layered onto generated image."""
    salon_name: str
    service_name: str
    price_pln: int | None = None       # 80, 250 etc.
    cta_text: str = "Zarezerwuj na Booksy"
    brand_color_hex: str = "#D4A574"   # BeautyAudit gold; salons can override
    logo_url: str | None = None        # if salon has thumbnail


async def generate_creative(
    prompt: str,
    *,
    size: OpenAISize = "1024x1024",
    quality: OpenAIQuality = "medium",
    n: int = 1,
    timeout: float = 60.0,
) -> list[bytes]:
    """Call gpt-image-1 → return raw PNG bytes per image.

    Returns list of bytes (one per requested image). Raises on API
    error so caller can retry / fail fast.
    """
    api_key = settings.openai_api_key
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not configured")

    payload = {
        "model": "gpt-image-1",
        "prompt": prompt,
        "n": n,
        "size": size,
        "quality": quality,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(
            "https://api.openai.com/v1/images/generations",
            json=payload, headers=headers,
        )
        if r.status_code != 200:
            raise RuntimeError(
                f"OpenAI image gen failed {r.status_code}: {r.text[:300]}"
            )
        data = r.json()

    images: list[bytes] = []
    for item in data.get("data", []):
        b64 = item.get("b64_json")
        if b64:
            images.append(base64.b64decode(b64))
    if len(images) != n:
        logger.warning(
            "gpt-image-1 returned %d/%d images (prompt len=%d)",
            len(images), n, len(prompt),
        )
    return images


def overlay_brand(
    image_bytes: bytes,
    overlay: CreativeOverlay,
) -> bytes:
    """Add salon name + service + price + CTA overlay to generated image.

    Style: bottom-third gradient → text → CTA pill bottom-right.
    Returns PNG bytes."""
    base = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    W, H = base.size
    draw_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(draw_layer)

    # Bottom gradient fade (transparent → dark) — improves text legibility
    gradient = Image.new("RGBA", (W, H // 3), (0, 0, 0, 0))
    g_draw = ImageDraw.Draw(gradient)
    for y in range(H // 3):
        alpha = int(230 * (y / (H // 3)) ** 1.5)
        g_draw.rectangle([(0, y), (W, y + 1)], fill=(0, 0, 0, alpha))
    draw_layer.paste(gradient, (0, H - H // 3), gradient)

    # Resolve fonts — fallback to PIL default if Outfit/Inter not available
    try:
        font_title = ImageFont.truetype("Outfit-Bold.ttf", size=int(W * 0.045))
        font_sub = ImageFont.truetype("Inter-Regular.ttf", size=int(W * 0.028))
        font_cta = ImageFont.truetype("Outfit-Bold.ttf", size=int(W * 0.030))
    except OSError:
        font_title = ImageFont.load_default(size=int(W * 0.045))
        font_sub = ImageFont.load_default(size=int(W * 0.028))
        font_cta = ImageFont.load_default(size=int(W * 0.030))

    pad_x = int(W * 0.05)
    text_y = H - int(H * 0.22)

    # Service name (large)
    draw.text(
        (pad_x, text_y), overlay.service_name,
        fill=(255, 255, 255, 255), font=font_title,
    )
    # Salon name + price (smaller, below)
    sub_text = overlay.salon_name
    if overlay.price_pln is not None:
        sub_text = f"{overlay.salon_name}  •  od {overlay.price_pln} zł"
    draw.text(
        (pad_x, text_y + int(H * 0.06)), sub_text,
        fill=(220, 220, 220, 255), font=font_sub,
    )

    # CTA pill — bottom right, brand color
    cta_text = overlay.cta_text
    cta_padding_x = int(W * 0.025)
    cta_padding_y = int(H * 0.014)
    bbox = draw.textbbox((0, 0), cta_text, font=font_cta)
    cta_w = bbox[2] - bbox[0] + cta_padding_x * 2
    cta_h = bbox[3] - bbox[1] + cta_padding_y * 2
    cta_x = W - cta_w - pad_x
    cta_y = H - cta_h - int(H * 0.05)
    rgb = tuple(int(overlay.brand_color_hex.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
    draw.rounded_rectangle(
        [(cta_x, cta_y), (cta_x + cta_w, cta_y + cta_h)],
        radius=int(cta_h / 2),
        fill=rgb + (255,),
    )
    draw.text(
        (cta_x + cta_padding_x, cta_y + cta_padding_y - 2),
        cta_text, fill=(255, 255, 255, 255), font=font_cta,
    )

    out = Image.alpha_composite(base, draw_layer).convert("RGB")
    buf = io.BytesIO()
    out.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


async def generate_ad_variants(
    salon_name: str,
    service_name: str,
    *,
    salon_category: str | None = None,
    price_pln: int | None = None,
    n_variants: int = 3,
    size: OpenAISize = "1024x1024",
) -> list[bytes]:
    """End-to-end: generate N gpt-image-1 variants + apply brand overlay.

    Each variant uses a slightly different prompt-style angle:
      0: "warm professional studio shot"
      1: "lifestyle authentic moment"
      2: "minimal editorial close-up"

    Tuned empirically for IG feed performance — variation in style
    matters more than variation in content for CTR diversity."""
    style_modifiers = [
        "warm professional studio photography, soft golden lighting, premium beauty editorial aesthetic",
        "authentic lifestyle moment, natural daylight, candid feel, in a beautiful salon interior",
        "minimal editorial close-up, neutral background, magazine-quality composition",
    ]
    category_hint = (
        f", premises and props consistent with {salon_category} salon" if salon_category else ""
    )

    base_prompt = (
        f"High-quality Polish beauty/medical aesthetic Instagram ad for "
        f"the service '{service_name}'{category_hint}. "
        f"Photorealistic, brand-safe, no text in image (text added later as overlay). "
        f"Professional appearance, single focal subject, clean composition, "
        f"warm colors, suitable for Polish beauty market in 2026."
    )

    tasks = [
        generate_creative(
            f"{base_prompt} Style: {style_modifiers[i % len(style_modifiers)]}",
            size=size,
            quality="medium",
            n=1,
        )
        for i in range(n_variants)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    overlay = CreativeOverlay(
        salon_name=salon_name,
        service_name=service_name,
        price_pln=price_pln,
    )

    final: list[bytes] = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            logger.warning("Variant %d failed: %s", i, r)
            continue
        if not r:
            continue
        try:
            final.append(overlay_brand(r[0], overlay))
        except Exception as e:  # noqa: BLE001
            logger.warning("Overlay failed for variant %d: %s", i, e)
    return final
