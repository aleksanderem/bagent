"""Synthetic edge-case ScrapedData fixtures + AI error injection helpers.

Why synthetic and not random sampling:
    Random sampling from prod misses pathological cases that we KNOW
    break pipelines (zero services, all-CAPS names, emoji-only, very
    long names, etc.). These fixtures pin the most dangerous shapes so
    every CI run sees them.

The error-injection helpers mock the MiniMax client to fail at
specific call indices, letting tests verify pipeline fallback paths
trigger correctly (e.g. quickWins deterministic fallback when AI
returns empty, retry/backoff when 503).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .conftest import _Category, _ScrapedData, _Service


# ---------------------------------------------------------------------------
# Edge-case ScrapedData fixtures
# ---------------------------------------------------------------------------

def edge_case_zero_services() -> _ScrapedData:
    """Salon with 0 services. Real-world: salons just listed, owner
    hasn't filled pricelist yet. Pipeline should produce a sensible
    summary like 'cennik wymaga uzupełnienia' instead of crashing."""
    return _ScrapedData(
        salonName="Pusty Salon Testowy",
        salonAddress="ul. Testowa 1, Warszawa",
        salonLogoUrl=None,
        totalServices=0,
        categories=[],
        salonCity="Warszawa",
        primaryCategoryId=None,
        primaryCategoryName=None,
        salonId="edge-zero-services",
    )


def edge_case_all_caps_names() -> _ScrapedData:
    """All service names in CAPS LOCK. Real-world: hundreds of salons
    in the wild have this. The fix_caps_lock helper should normalize
    them, so transformations should reflect that fix."""
    return _ScrapedData(
        salonName="STUDIO BEAUTY ALL CAPS",
        salonAddress="ul. Wielkich Liter 5, Kraków",
        salonLogoUrl=None,
        totalServices=4,
        categories=[
            _Category(
                name="FRYZJERSTWO",
                services=[
                    _Service(name="STRZYŻENIE DAMSKIE", price="80 zł", duration="45 min"),
                    _Service(name="STRZYŻENIE MĘSKIE", price="60 zł", duration="30 min"),
                ],
            ),
            _Category(
                name="KOSMETYKA",
                services=[
                    _Service(name="MANICURE HYBRYDOWY", price="120 zł", duration="60 min"),
                    _Service(name="PEDICURE LECZNICZY", price="150 zł", duration="75 min"),
                ],
            ),
        ],
        salonCity="Kraków",
        primaryCategoryId=None,
        primaryCategoryName="Salon kosmetyczny",
        salonId="edge-all-caps",
    )


def edge_case_emoji_in_names() -> _ScrapedData:
    """Service names contain emoji. Pipeline must not crash on Unicode
    in name field; transformations may strip emoji or keep them."""
    return _ScrapedData(
        salonName="✨ Beauty Studio ✨",
        salonAddress="ul. Emoji 1, Wrocław",
        salonLogoUrl=None,
        totalServices=3,
        categories=[
            _Category(
                name="💅 Manicure",
                services=[
                    _Service(name="💎 Manicure hybrydowy", price="100 zł", duration="60 min"),
                    _Service(name="🌺 Pedicure SPA", price="150 zł", duration="90 min"),
                    _Service(name="✨ French", price="120 zł", duration="75 min"),
                ],
            ),
        ],
        salonCity="Wrocław",
        primaryCategoryId=None,
        primaryCategoryName=None,
        salonId="edge-emoji",
    )


def edge_case_very_long_names() -> _ScrapedData:
    """Service names >200 chars. Real-world: people paste full marketing
    copy as service name. PDF rendering must not overflow; AI naming
    agent should compress."""
    long_name = (
        "Profesjonalne strzyżenie damskie z myciem włosów ekologicznym "
        "szamponem, modelowaniem suszarką i lokówką, plus pełna "
        "stylizacja włosów do okazji wieczornych z lakierem trwałym "
        "i pielęgnacją końcówek olejkiem arganowym oraz"
    )
    return _ScrapedData(
        salonName="Salon Z Bardzo Długimi Nazwami",
        salonAddress="ul. Dluga 1, Gdańsk",
        salonLogoUrl=None,
        totalServices=2,
        categories=[
            _Category(
                name="Fryzjerstwo profesjonalne premium luxury",
                services=[
                    _Service(name=long_name, price="200 zł", duration="120 min"),
                    _Service(name="Krótka usługa", price="50 zł", duration="30 min"),
                ],
            ),
        ],
        salonCity="Gdańsk",
        primaryCategoryId=None,
        primaryCategoryName=None,
        salonId="edge-long-names",
    )


def edge_case_duplicate_names() -> _ScrapedData:
    """Multiple services with identical name. Pipeline's dedup logic
    must not merge them away (each may have different price/duration)
    and the audit must flag this as an issue."""
    return _ScrapedData(
        salonName="Salon Z Duplikatami",
        salonAddress="ul. Duplikat 1, Łódź",
        salonLogoUrl=None,
        totalServices=4,
        categories=[
            _Category(
                name="Manicure",
                services=[
                    _Service(name="Manicure", price="80 zł", duration="60 min"),
                    _Service(name="Manicure", price="100 zł", duration="75 min"),  # dupe
                    _Service(name="Pedicure", price="120 zł", duration="90 min"),
                    _Service(name="Pedicure", price="120 zł", duration="90 min"),  # exact dupe
                ],
            ),
        ],
        salonCity="Łódź",
        primaryCategoryId=None,
        primaryCategoryName=None,
        salonId="edge-duplicates",
    )


def edge_case_empty_descriptions() -> _ScrapedData:
    """Every service has price + duration but NO description. The
    description-agent should generate descriptions; coverage should
    show high `optimized` count."""
    return _ScrapedData(
        salonName="Salon Bez Opisów",
        salonAddress="ul. Pusta 1, Poznań",
        salonLogoUrl=None,
        totalServices=5,
        categories=[
            _Category(
                name="Fryzjerstwo",
                services=[
                    _Service(name="Strzyżenie damskie", price="80 zł", duration="45 min", description=None),
                    _Service(name="Koloryzacja", price="200 zł", duration="120 min", description=None),
                    _Service(name="Modelowanie", price="60 zł", duration="30 min", description=None),
                    _Service(name="Trwała", price="180 zł", duration="180 min", description=None),
                    _Service(name="Pielęgnacja włosów", price="100 zł", duration="60 min", description=None),
                ],
            ),
        ],
        salonCity="Poznań",
        primaryCategoryId=None,
        primaryCategoryName=None,
        salonId="edge-empty-descriptions",
    )


def edge_case_huge_pricelist() -> _ScrapedData:
    """100+ services across many categories. Tests that the audit
    pipeline (a) doesn't OOM, (b) finishes in < N min, (c) emits
    coherent transformations across all categories."""
    cats = []
    for ci in range(10):
        services = [
            _Service(
                name=f"Usługa {ci}.{i} testowa kosmetyczna",
                price=f"{50 + i * 10} zł",
                duration=f"{30 + i * 5} min",
                description=None if i % 3 == 0 else f"Opis usługi {ci}.{i}",
            )
            for i in range(12)
        ]
        cats.append(_Category(name=f"Kategoria {ci}", services=services))

    return _ScrapedData(
        salonName="Mega Salon Z Cennikiem 120",
        salonAddress="ul. Duża 1, Warszawa",
        salonLogoUrl=None,
        totalServices=120,
        categories=cats,
        salonCity="Warszawa",
        primaryCategoryId=None,
        primaryCategoryName="Salon kosmetyczny",
        salonId="edge-huge-pricelist",
    )


# Registry — used by edge_case_salons fixture
ALL_EDGE_CASES: list[Callable[[], _ScrapedData]] = [
    edge_case_zero_services,
    edge_case_all_caps_names,
    edge_case_emoji_in_names,
    edge_case_very_long_names,
    edge_case_duplicate_names,
    edge_case_empty_descriptions,
    edge_case_huge_pricelist,
]


# ---------------------------------------------------------------------------
# AI error injection helpers
# ---------------------------------------------------------------------------

@dataclass
class FailingMiniMaxClient:
    """Drop-in replacement for ``services.minimax.MiniMaxClient`` that
    fails at specific call indices so tests can verify pipeline
    fallback paths.

    Usage:
        fake = FailingMiniMaxClient(fail_indices={2, 5})
        # Calls 0, 1, 3, 4 succeed; calls 2 and 5 raise httpx.TimeoutError.

    fail_modes can override per-index error type:
        fake = FailingMiniMaxClient(
            fail_indices={0},
            fail_modes={0: "503"},       # raises Exception with '503' in message
        )
    """

    fail_indices: set[int] = field(default_factory=set)
    fail_modes: dict[int, str] = field(default_factory=dict)
    success_response: Any = field(default_factory=lambda: '{"score": 50, "issues": []}')
    call_log: list[tuple[str, dict]] = field(default_factory=list)
    _call_index: int = 0

    def _next(self, kind: str, kwargs: dict) -> Any:
        idx = self._call_index
        self._call_index += 1
        self.call_log.append((kind, kwargs))
        if idx in self.fail_indices:
            mode = self.fail_modes.get(idx, "timeout")
            if mode == "timeout":
                import httpx
                raise httpx.TimeoutException(f"injected timeout at call#{idx}")
            elif mode == "503":
                raise Exception(f"503 Service Unavailable (injected at call#{idx})")
            elif mode == "invalid_json":
                return "this is not json {{"
            elif mode == "empty":
                return "" if kind == "text" else {}
            else:
                raise Exception(f"injected {mode} at call#{idx}")
        return self.success_response if kind == "text" else {"score": 50, "issues": []}

    async def generate_text(self, prompt: str, **kwargs: Any) -> str:
        return self._next("text", {"prompt": prompt[:100], **kwargs})

    async def generate_json(self, prompt: str, **kwargs: Any) -> dict:
        return self._next("json", {"prompt": prompt[:100], **kwargs})


def fail_at_calls(*indices: int, mode: str = "timeout") -> FailingMiniMaxClient:
    """Convenience factory for the most common case."""
    return FailingMiniMaxClient(
        fail_indices=set(indices),
        fail_modes={i: mode for i in indices},
    )
