#!/usr/bin/env bash
# The release launch check (src/stack/smoke.js) on the package a build left in
# dist/$DESKTOP_EDITION: the bundled stack started as a first launch starts it,
# and the app's exit code is the answer.
#
#   scripts/smoke-packaged.sh [profile-dir]
#
# Skips, with exit 0, a package that carries no bundled backend (a thin build,
# Windows), since there is no stack in it to start. The profile is the app's
# --user-data-dir, a fresh temporary one when none is given.
set -euo pipefail

cd "$(dirname "$0")/.."
dist="dist/${DESKTOP_EDITION:-oss}"
profile="${1:-$(mktemp -d)}"
# A first launch initializes the database and runs every migration.
limit=900

say() {
  local level=$1
  shift
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::$level::$*"; else echo "[smoke] $*"; fi
}
skip() { say notice "$* There is no stack to start, so the launch check is skipped."; exit 0; }
fail() { say error "$*"; exit 1; }

[ -d "$dist" ] || fail "nothing was built in $dist"
export LANGALPHA_DESKTOP_SMOKE=1
# A dev shell's overrides would point the app at a checkout's tree or swap the
# package's database and Valkey, and the check would pass without the package.
unset LANGALPHA_DESKTOP_BACKEND_TREE LANGALPHA_DESKTOP_PG_URL LANGALPHA_DESKTOP_PG_PREFIX \
  LANGALPHA_DESKTOP_PG_MAJOR LANGALPHA_DESKTOP_VALKEY_BIN
case "$(uname -s)" in
  Darwin)
    app=$(find "$dist" -maxdepth 2 -name '*.app' -print -quit)
    [ -n "$app" ] || fail "no .app was built in $dist"
    [ -f "$app/Contents/Resources/backend/manifest.json" ] || skip "$app carries no bundled backend."
    exe="$app/Contents/MacOS/$(/usr/libexec/PlistBuddy -c 'Print :CFBundleExecutable' "$app/Contents/Info.plist")"
    # macOS ships no `timeout`.
    exec perl -e 'alarm shift; exec @ARGV' "$limit" "$exe" --user-data-dir="$profile"
    ;;
  Linux)
    image=$(find "$dist" -maxdepth 1 -name '*.AppImage' -print -quit)
    [ -n "$image" ] || fail "no AppImage was built in $dist"
    # What electron-builder packed into the image, left beside it unpacked.
    trees=("$dist"/*-unpacked/resources/backend/manifest.json)
    [ -f "${trees[0]}" ] || skip "$image carries no bundled backend."
    chmod +x "$image"
    # Extracted rather than mounted, since a CI runner has no FUSE 2, and without
    # Chromium's sandbox, which needs a setuid helper an extracted image does not keep.
    exec timeout "$limit" xvfb-run -a "$image" --appimage-extract-and-run --no-sandbox --user-data-dir="$profile"
    ;;
  *)
    skip "No package on $(uname -s) carries a bundled backend."
    ;;
esac
