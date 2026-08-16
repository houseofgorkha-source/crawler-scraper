# Production scheduling — Linux deployment only

This file documents the cron schedule the architecture assumes for two
maintenance jobs. It is **documentation for a real Linux deployment target**,
not something configured on this repository's Windows development machine.
Nothing here has been installed, scheduled, or otherwise activated locally —
see `CLAUDE.md`/`README.md` for what's actually running in dev (Docker
Compose infra only, jobs invoked by hand or from tests).

## `reap` — lease reaper

**Spec** (`CLAUDE.md`, "Non-negotiable design decisions"): *"a reaper
(`reap_expired_leases()`, run every 60s) returns unrenewed leases to the
pool."*

`python -m crawler.cli reap` is a one-shot invocation — it calls
`reap_expired_leases()` and `reap_expired_scrape_leases()` once and exits
(`crawler/cli.py: cmd_reap`). It is not a long-running loop, so the 60s
cadence has to come from the scheduler, not the process itself.

Standard cron only resolves to whole minutes, so a 60s interval is exactly
representable as a `* * * * *` line:

```cron
# /etc/cron.d/crawler-reap  (production Linux host only)
* * * * * crawler_svc  cd /opt/crawler && CRAWLER_PG_DSN=... /opt/crawler/.venv/bin/python -m crawler.cli reap >> /var/log/crawler/reap.log 2>&1
```

If sub-minute precision or process supervision (auto-restart, structured
logging, dependency ordering on Postgres being up) matters more than cron's
simplicity, a systemd timer unit firing every 60s is the documented
alternative for the same command — not a different job, just a different
scheduler.

## `ops/partitions.sql` — monthly partition + retention

**Spec** (`ops/partitions.sql` header, `CLAUDE.md`): *"`crawl_attempts` is
monthly-partitioned and pruned after 30 days (`ops/partitions.sql`, meant to
run from cron)."* The script itself creates next month's partition and drops
any partition older than the 30-day retention window — it is idempotent
(`CREATE TABLE IF NOT EXISTS`) and safe to run more often than strictly
necessary, but monthly is what the design calls for.

```cron
# /etc/cron.d/crawler-partitions  (production Linux host only)
# Run once a month, comfortably before the next partition boundary.
0 3 1 * * postgres  psql -U postgres -d crawler -f /opt/crawler/ops/partitions.sql >> /var/log/crawler/partitions.log 2>&1
```

## Why this isn't wired up in this repository

Per explicit instruction: this repository's working environment is Windows
and is not the deployment target these schedules assume. No Windows Task
Scheduler entries, systemd units, or crontabs were created on this machine.
This file exists so the schedule is captured once, correctly, rather than
re-derived at actual deployment time — apply it on whatever Linux host runs
`crawl`/`scrape`/`index` in production.
