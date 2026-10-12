#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "Docker Desktop with Docker Compose v2 is required." >&2
  exit 1
fi
if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example to .env and replace every credential placeholder first." >&2
  exit 1
fi
if grep -q 'replace-with-' .env; then
  echo "Unchanged credential placeholder found in .env. Use unique local passwords and token before startup." >&2
  exit 1
fi

docker compose config --quiet
docker compose up -d --build
docker compose ps
printf '\nCryptoSentinel dashboard: http://127.0.0.1:8000\nGrafana:                  http://127.0.0.1:3000\nNeo4j Browser:             http://127.0.0.1:7474\nInfluxDB UI:               http://127.0.0.1:8086\n'
