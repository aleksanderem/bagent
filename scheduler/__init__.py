"""Issue #23 — scrape orchestrator scheduler.

Runs once an hour (under arq cron) and tops up `salon_refresh_queue` with
salons whose tier-specific cadence has elapsed. The actual scraping is
done by `bagent/workers/scrape_refresh.py` which consumes the queue.
"""

from .refresh_scheduler import schedule_due_refreshes

__all__ = ["schedule_due_refreshes"]
