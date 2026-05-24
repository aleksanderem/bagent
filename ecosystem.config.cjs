// PM2 ecosystem config — uses scripts/run_with_env.sh wrapper to source
// .env before exec (PM2 v6.0.14 nie ma env_file natywnie). Without this
// wrapper, worker runs with empty env and pipelines crash on missing
// OPENAI_API_KEY / SUPABASE_URL etc. Naprawa 2026-05-20 incident.
module.exports = {
  apps: [
    {
      name: "bagent-worker",
      script: "/home/booksy/webapps/bagent-booksyauditor/scripts/run_with_env.sh",
      // --log-level info exposes the per-phase logger.info markers from
      // pipelines/competitor_analysis.py + services/* in PM2 logs. Without
      // it arq defaults to WARNING which swallowed the entire 649s opaque
      // middle block in profile 2026-05-24-pipeline-profile.md.
      args: "/home/booksy/.local/bin/uv run arq workers.WorkerSettings --log-level info",
      cwd: "/home/booksy/webapps/bagent-booksyauditor",
      autorestart: true,
      max_restarts: 50,
      restart_delay: 5000,
    },
    {
      name: "bagent-booksyauditor",
      script: "/home/booksy/webapps/bagent-booksyauditor/scripts/run_with_env.sh",
      args: "/home/booksy/.local/bin/uv run uvicorn server:app --host 0.0.0.0 --port 3003",
      cwd: "/home/booksy/webapps/bagent-booksyauditor",
      autorestart: true,
      max_restarts: 50,
      restart_delay: 5000,
    },
  ],
};
