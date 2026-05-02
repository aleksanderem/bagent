"""Issue #34 — Booksy salon discovery via quad-tree bbox subdivision.

Booksy listing API caps results at 20 per request and ignores ?page=N.
The only paginator is the geographic ?area= parameter. To enumerate
all salons in a (category, voivodeship) combo we recursively subdivide
the voivodeship bounding box: if a probe returns >20 salons we split
the bbox into 4 quadrants and recurse into each.

Discovered salons are UPSERT'd into `discovered_salons` (PK
booksy_id) so cross-bbox overlaps deduplicate. The orchestrator
in `salon_refresh_queue` then pulls full salon details lazily.

Public API:
    discover_combo(category_id, voivodeship_id) -> DiscoveryResult
    discover_all() -> list[DiscoveryResult]
"""

from .discover import (
    DiscoveryResult,
    discover_all,
    discover_combo,
)

__all__ = ["DiscoveryResult", "discover_all", "discover_combo"]
