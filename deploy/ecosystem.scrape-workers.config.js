// Issue #23 — PM2 ecosystem for scrape orchestrator workers.
//
// Two consumer processes share a single Redis-backed arq queue and a
// single Postgres-backed `salon_refresh_queue` table. Each gets a
// unique SCRAPE_WORKER_ID so the `locked_by` column disambiguates
// who claimed which job.
//
// Deploy:
//   pm2 start /home/booksy/bagent/deploy/ecosystem.scrape-workers.config.js
//   pm2 save
//
// Stop both:
//   pm2 stop scrape-worker-1 scrape-worker-2
//
// At higher scale, switch to a "one cron leader + N consumers" model
// to avoid duplicate cron firings (see ops/scrape-orchestrator/RUNBOOK.md).

module.exports = {
  apps: [
    {
      name: "scrape-worker-1",
      script: "uv",
      args: "run arq workers.WorkerSettings",
      cwd: "/home/booksy/bagent",
      env: {
        SCRAPE_WORKER_ID: "scrape-worker-1",
      },
      kill_timeout: 30000,
      autorestart: true,
      max_memory_restart: "500M",
    },
    {
      name: "scrape-worker-2",
      script: "uv",
      args: "run arq workers.WorkerSettings",
      cwd: "/home/booksy/bagent",
      env: {
        SCRAPE_WORKER_ID: "scrape-worker-2",
      },
      kill_timeout: 30000,
      autorestart: true,
      max_memory_restart: "500M",
    },
  ],
};
