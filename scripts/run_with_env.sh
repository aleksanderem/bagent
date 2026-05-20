#!/usr/bin/env bash
# Sources .env then exec target binary with all its args.
# PM2 v6.0.14 nie ma env_file natywnie. Bez tego wrappera worker
# uruchamia się z pustym env i pipelines crashują na OPENAI_API_KEY /
# SUPABASE_URL etc. Naprawa po 2026-05-20 incident.
set -a
. /home/booksy/webapps/bagent-booksyauditor/.env
set +a
cd /home/booksy/webapps/bagent-booksyauditor
exec "$@"
