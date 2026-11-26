#!/usr/bin/env bash
# Uploads the feed feed-gather.py laid out in `feed/`: every archive the
# manifests name, then the manifests, which is the moment installed apps learn of
# the build. Run from the directory holding `feed/`.
#
#   env  CHANNEL           whose manifests to publish (latest, nightly, ...)
#        FEED              the public address the bucket is served at
#        R2_ENDPOINT, R2_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#        OVERWRITE=true    replace an archive already there with other bytes
set -eo pipefail

tmp=${RUNNER_TEMP:-$(mktemp -d)}

probe () {
  local url="$1" want="$2" hdrs code cache
  hdrs=$(curl -sSLI --max-time 60 "$url" 2>/dev/null | tr -d '\r' || true)
  code=$(printf '%s\n' "$hdrs" | awk 'toupper($1) ~ /^HTTP/ {c=$2} END {print c+0}')
  cache=$(printf '%s\n' "$hdrs" | awk -F': ' 'tolower($1)=="cache-control" {v=$2} END {print tolower(v)}')
  [ "$code" = 200 ] || { echo "::error::$url answered $code"; return 1; }
  case "$cache" in
    *"$want"*) echo "  ok   ${url##*/}  ($cache)" ;;
    *) echo "::error::$url is cached as '${cache:-nothing}', expected $want"; return 1 ;;
  esac
}
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY R2_ENDPOINT R2_BUCKET; do
  [ -n "${!v}" ] || { echo "::error::a feed is configured but $v is not set"; exit 1; }
done
payload=()
while IFS= read -r f; do payload+=("$f"); done < <(
  sed -nE 's/^[[:space:]]*-?[[:space:]]*url:[[:space:]]*//p' feed/"$CHANNEL"*.yml \
    | tr -d '\r' | sed -E "s/^['\"]//; s/['\"]\$//" | sort -u
)
[ "${#payload[@]}" -gt 0 ] || { echo "::error::the $CHANNEL manifest names no files"; exit 1; }

# Archives are served immutable for a year, so the same name with other
# bytes would leave every cached copy disagreeing with the manifest's
# checksum. The same bytes again, a rerun of this job, are skipped, which
# is also how a run that stopped between the archives and the manifests
# is finished: rerun it. Only a 404 counts as absent: an object this run
# cannot read, or one with no checksum of ours, is never replaced
# without overwrite.
put () {
  local file="feed/$1" sum head remote
  sum=$(sha256sum "$file" | cut -d' ' -f1)
  if head=$(aws s3api head-object --bucket "$R2_BUCKET" --key "$1" --endpoint-url "$R2_ENDPOINT" \
      --output json 2>"$tmp/head.err"); then
    remote=$(printf '%s' "$head" | jq -r '.Metadata.sha256 // empty')
    if [ "$remote" = "$sum" ]; then echo "  same  $1"; return 0; fi
    if [ "$OVERWRITE" != true ]; then
      local why=${remote:+with other bytes}
      echo "::error::$1 is already in the feed ${why:-with no checksum to compare}. Bump the version, or run with overwrite if no one can have fetched it."
      return 1
    fi
  elif ! grep -q '(404)' "$tmp/head.err"; then
    cat "$tmp/head.err"
    echo "::error::could not tell whether $1 is already in the feed"
    return 1
  fi
  aws s3 cp "$file" "s3://$R2_BUCKET/$1" --endpoint-url "$R2_ENDPOINT" --metadata "sha256=$sum" --only-show-errors
  echo "  put   $1"
}

for f in "${payload[@]}"; do
  [ -f "feed/$f" ] || { echo "::error::the $CHANNEL manifest names $f, which this run did not build"; exit 1; }
  put "$f"
  if [ -f "feed/$f.blockmap" ]; then put "$f.blockmap"; fi
done
# One archive through the edge before any manifest names it: the bytes
# landed behind the public address, and are cached as immutable.
probe "$FEED/${payload[0]}" immutable
# Last: this is the moment the release reaches installed apps.
for m in feed/"$CHANNEL"*.yml; do
  name=$(basename "$m")
  aws s3 cp "$m" "s3://$R2_BUCKET/$name" --endpoint-url "$R2_ENDPOINT" --only-show-errors
  echo "  put   $name"
done
