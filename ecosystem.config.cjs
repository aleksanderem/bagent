// PM2 ecosystem config — uses scripts/run_with_env.sh wrapper to source
// .env before exec (PM2 v6.0.14 nie ma env_file natywnie). Without this
// wrapper, worker runs with empty env and pipelines crash on missing
// OPENAI_API_KEY / SUPABASE_URL etc. Naprawa 2026-05-20 incident.
module.exports = {
  apps: [
    {
      name: "bagent-worker",
      script: "/home/booksy/webapps/bagent-booksyauditor/scripts/run_with_env.sh",
      // INFO-level logging for pipelines/competitor_analysis.py +
      // services/* is enabled programmatically via workers/main.py
      // startup() — sets logging.getLogger("pipelines"|"services"|
      // "agent").setLevel(logging.INFO). arq's CLI does NOT accept
      // --log-level (tried 2026-05-24, crashloops with "No such
      // option: --log-level"); setting subloggers at startup is the
      // only reliable path.
      args: "/home/booksy/.local/bin/uv run arq workers.WorkerSettings",
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
