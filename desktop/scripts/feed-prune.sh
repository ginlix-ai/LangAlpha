#!/usr/bin/env bash
# Deletes the feed's nightly archives past the newest KEEP versions. Only names
# carrying a nightly version are ever touched, so a stable release's files and
# every manifest stay. An installed nightly older than the ones kept still
# updates: it reads the current manifest and fetches the whole archive rather
# than the changed blocks.
#
#   env  KEEP   how many nightly versions to keep (at least 1)
#        R2_ENDPOINT, R2_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
set -eo pipefail

[ "${KEEP:-0}" -ge 1 ] || { echo "::error::KEEP must be at least 1"; exit 1; }

keys=$(aws s3api list-objects-v2 --bucket "$R2_BUCKET" --endpoint-url "$R2_ENDPOINT" \
  --query 'Contents[].Key' --output text | tr '\t' '\n' | grep -v '^None$' || true)
# The stamp after `nightly.` is the build's UTC time, so it orders them.
versions=$(printf '%s\n' "$keys" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+-nightly\.[0-9]+' \
  | sort -u | awk -F'nightly.' '{print $2, $0}' | sort -n | cut -d' ' -f2)
total=$(printf '%s\n' "$versions" | grep -c . || true)
if [ "$total" -le "$KEEP" ]; then
  echo "  $total nightly versions in the feed, keeping all"
  exit 0
fi

printf '%s\n' "$versions" | head -n $((total - KEEP)) | while IFS= read -r version; do
  # Anchored on both sides, so one version is never a prefix of another's name.
  printf '%s\n' "$keys" | grep -E -- "-${version//./\\.}([^0-9]|$)" | while IFS= read -r key; do
    aws s3 rm "s3://$R2_BUCKET/$key" --endpoint-url "$R2_ENDPOINT" --only-show-errors
    echo "  rm    $key"
  done
done
