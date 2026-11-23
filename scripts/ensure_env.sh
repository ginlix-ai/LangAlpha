#!/usr/bin/env bash
# Gives a fresh checkout the .env that docker-compose.yml reads, with an
# encryption key of its own. Only when creating the file: data already in a
# database is encrypted under whatever key an existing .env led it to use, and
# replacing that key would make it unreadable.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

[ -f "$ENV_FILE" ] && exit 0

if command -v openssl >/dev/null 2>&1; then
    key="$(openssl rand -hex 32)"
else
    key="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
fi

# Written whole and then moved, so an interrupted run leaves no half-made .env
# that the next run would take for an existing one.
tmp="$ENV_FILE.tmp.$$"
trap 'rm -f "$tmp"' EXIT
umask 077
grep -v '^# *BYOK_ENCRYPTION_KEY=' "$REPO_ROOT/.env.example" > "$tmp"
printf '\nBYOK_ENCRYPTION_KEY=%s\n' "$key" >> "$tmp"
mv "$tmp" "$ENV_FILE"
echo "Created .env with a new encryption key."
