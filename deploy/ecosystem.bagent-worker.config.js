// PM2 ecosystem entry for the bagent arq worker process.
//
// Deploy on tytan as user `booksy`:
//   cd /home/booksy/webapps/bagent-booksyauditor
//   sudo -u booksy -H bash -lc 'pm2 start deploy/ecosystem.bagent-worker.config.js'
//   sudo -u booksy -H bash -lc 'pm2 save'
//
// The web process (bagent-booksyauditor, id 8) keeps running uvicorn
// server:app. This new entry runs the arq worker pool. Both processes
// share the same Redis instance (127.0.0.1:6379 from
// docker-compose.redis.yml).
//
// Observability:
//   pm2 logs bagent-worker --lines 100
//   pm2 describe bagent-worker
//
// Restart safety: arq workers can be SIGINT'd (graceful) or SIGKILL'd
// (forceful). On graceful shutdown, in-flight jobs are returned to the
// queue and retried by another worker (subject to max_tries). PM2's
// default kill_timeout=1600ms is short — bumped here so workers have
// time to finish their current task on shutdown.

module.exports = {
  apps: [
    {
      name: "bagent-worker",
      cwd: "/home/booksy/webapps/bagent-booksyauditor",
      script: "/home/booksy/.local/bin/uv",
      args: "run arq workers.WorkerSettings",
      interpreter: "none",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      kill_timeout: 30000, // 30s for graceful arq shutdown
      env: {
        // arq + bagent read settings from .env in cwd, no override needed
        // unless you want to point a worker at a different Redis.
      },
      out_file: "/home/booksy/.pm2/logs/bagent-worker-out.log",
      error_file: "/home/booksy/.pm2/logs/bagent-worker-error.log",
      merge_logs: true,
      time: true,
    },
  ],
};
