#!/usr/bin/env bash
# The manifests in `feed/` reached the way an installed app reaches them. The
# cache header is half the check: a manifest served as immutable is read from
# cache forever, and every copy goes on reporting itself up to date.
#
#   env  CHANNEL, FEED   as for feed-publish.sh
set -eo pipefail

fail=0
for m in feed/"$CHANNEL"*.yml; do
  url="$FEED/$(basename "$m")"
  hdrs=$(curl -sSLI --max-time 60 "$url" 2>/dev/null | tr -d '\r' || true)
  code=$(printf '%s\n' "$hdrs" | awk 'toupper($1) ~ /^HTTP/ {c=$2} END {print c+0}')
  cache=$(printf '%s\n' "$hdrs" | awk -F': ' 'tolower($1)=="cache-control" {v=$2} END {print tolower(v)}')
  case "$code $cache" in
    "200 "*no-store*) echo "  ok   ${url##*/}  ($cache)" ;;
    *) echo "::error::$url answered $code, cached as '${cache:-nothing}'"; fail=1 ;;
  esac
done
exit $fail
