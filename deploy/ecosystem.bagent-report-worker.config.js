// PM2 ecosystem entry for the DEDICATED competitor-report arq worker.
//
// Isolated from the scrape pump (2026-06-15): runs ReportWorkerSettings on
// its own "arq:reports" queue, nice'd (CPU) + ionice'd (IO best-effort low),
// so a heavy report or classification-backfill load can't starve the scrape
// worker — and the scrape pump can't starve report latency (the stuck-worker
// incident where reports cycled >1h under shared load). Shares Redis +
// Supabase + .env with bagent-worker; DB contention is mitigated, not
// eliminated, so keep classification-backfill concurrency low.
//
// The competitor_report_queue drain + reap crons live in ReportWorkerSettings
// (moved off SCRAPE_CRONS), so ONLY this process drains the queue and enqueues
// run_competitor_report_task to "arq:reports".
//
// Deploy on tytan as user `booksy`:
//   cd /home/booksy/webapps/bagent-booksyauditor
//   sudo -u booksy -i bash -c 'source ~/.nvm/nvm.sh && \
//     pm2 start deploy/ecosystem.bagent-report-worker.config.js && pm2 save'
//
// scripts/run_with_env.sh sources .env (PM2 has no native env_file) then execs
// the nice/ionice-wrapped arq worker — mirrors the bagent-worker launch.
//
//   pm2 logs bagent-report-worker --lines 100
//   pm2 describe bagent-report-worker
module.exports = {
  apps: [
    {
      name: "bagent-report-worker",
      cwd: "/home/booksy/webapps/bagent-booksyauditor",
      script: "/home/booksy/webapps/bagent-booksyauditor/scripts/run_with_env.sh",
      args: "nice -n 10 ionice -c2 -n7 /home/booksy/.local/bin/uv run arq workers.ReportWorkerSettings",
      interpreter: "bash",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      kill_timeout: 30000, // 30s for graceful arq shutdown (in-flight report)
      env: {},
    },
  ],
};
