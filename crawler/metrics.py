"""
Shared Prometheus metrics for Crawler and Scraper. Both workers import
from here rather than defining their own registries, so a single
--metrics-port in either process exposes everything relevant to it.

Deliberately minimal: outcome counters + fetch/render duration. Add more
only when an actual operational question can't be answered without them.
"""
from __future__ import annotations

from prometheus_client import Counter, Histogram

CRAWL_TASKS = Counter(
    "crawler_tasks_total", "Crawl tasks by terminal outcome", ["outcome"]
)
SCRAPE_TASKS = Counter(
    "scraper_tasks_total", "Scrape tasks by terminal outcome", ["outcome"]
)
FETCH_DURATION_SECONDS = Histogram(
    "crawler_fetch_duration_seconds",
    "Fetch/render duration by subsystem and render mode",
    ["subsystem", "render_mode"],
)
