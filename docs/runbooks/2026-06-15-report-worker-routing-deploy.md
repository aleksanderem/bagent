# Deploy runbook — route audits + on-demand reports to the dedicated report worker

Date: 2026-06-15
Change: `feat/audit-dedicated-report-worker-260615` (PR #39) + `fix/audit-progress-feedback-260615` (PR #38, already on main).

## What changed

`server._enqueue_pipeline` now enqueues every user-facing report/LLM job
(audit `run_report_task`, free report, cennik, summary, versum suggest,
competitor refresh) with `_queue_name="arq:reports"` instead of the default
`arq:queue`. The dedicated `bagent-report-worker` (`ReportWorkerSettings`,
queue `arq:reports`) now registers `*ALL_TASKS` and consumes them. The scrape
worker (`bagent-worker`, `arq:queue`) keeps the tasks registered only so any
in-flight job enqueued **before** the deploy can still finish/retry.

Net: scraping runs alone on `bagent-worker`; all audits/reports run on
`bagent-report-worker`.

## ⛔ Why restart order matters (CRITICAL)

The three processes deploy independently. If the **API** (`bagent-booksyauditor`)
restarts with the new routing **before** the report worker is on the new code,
the OLD report worker pops `run_report_task` off `arq:reports`, finds the
function unregistered, and arq calls `finish_failed_job` — the job is
**permanently dropped** (no retry, no fail webhook → the paid audit silently
hangs forever).

`pm2 restart all` / `pm2 reload all` restarts in pm_id order
(`bagent-booksyauditor` = 2 **before** `bagent-report-worker` = 5) → it triggers
this hazard by default. **Do NOT use `pm2 restart all`.**

The reverse order (report worker new, API old) is harmless: the old API enqueues
to `arq:queue`, drained by the scrape worker which still registers the tasks.

## Pre-flight

```bash
ssh -i ~/.ssh/id_rsa_alex root@tytan
sudo -u booksy -i bash -c 'source ~/.nvm/nvm.sh && pm2 describe bagent-report-worker' | grep -E 'status|uptime'
# MUST be online. If not: pm2 start deploy/ecosystem.bagent-report-worker.config.js && pm2 save
```

## Deploy (exact order)

Run as user `booksy` (the `.git` checkout is owned by booksy:booksy):

```bash
cd /home/booksy/webapps/bagent-booksyauditor
sudo -u booksy git pull            # fast-forward/merge origin/main (PR #38 + #39)

source ~/.nvm/nvm.sh

# 1) REPORT WORKER FIRST — load new code (run_report_task registered on
#    arq:reports) + the 2G memory bump + report_worker_heartbeat, BEFORE any
#    audit can be routed there. delete+start re-reads the config file.
pm2 delete bagent-report-worker
pm2 start deploy/ecosystem.bagent-report-worker.config.js
pm2 save
#    verify it booted on arq:reports with the report functions:
pm2 logs bagent-report-worker --lines 30 --nostream | grep -E 'redis PING|shared clients ready|Starting worker'

# 2) API SECOND — only now does it begin routing to arq:reports.
pm2 restart bagent-booksyauditor

# 3) SCRAPE WORKER anytime (picks up the report.py progress fix; never
#    consumes arq:reports).
pm2 restart bagent-worker
```

## Post-deploy verification

```bash
# all three online
pm2 list | grep -E 'bagent'

# arq:reports queue depth should trend to 0, not grow (no stranded jobs)
redis-cli -n <bagent_db> zcard arq:reports     # or via docker exec if redis is containerized

# fire one real audit from the app, then confirm it executed on the report worker:
pm2 logs bagent-report-worker --lines 50 --nostream | grep -E 'run_report_task|PROGRESS|naming'
# the progress bar should creep (per-step) and never jump backward (PR #38).
```

## Monitoring follow-up (not blocking, but do it)

`report_worker_heartbeat` pings `HC_PING_BAGENT_REPORT_WORKER_HEARTBEAT` every
5 min. Create that Healthchecks check (~10 min grace) and set the env var in
`.env`, else a dead report worker won't page (the scrape worker's heartbeat says
nothing about it). Until then the ping is a graceful no-op.

## Rollback

```bash
cd /home/booksy/webapps/bagent-booksyauditor
sudo -u booksy git revert <merge-sha>   # or check out the prior commit
# redeploy in the SAME order (report worker first, then API).
```
Routing reverts cleanly: with the API on old code, audits go back to `arq:queue`
and the scrape worker (still registers the tasks) runs them. No data migration.
