# Makes the pinned Claude Code CLI ready on this machine; runs after LOCK_SH
# and IS_MUSL_SH, which define lock() and is_musl().
#
# Arguments: the CLI's folder, the pinned version, the release URL, the path
# a copy sent from the backend was uploaded to (or empty), 1 to install or 0
# only to look, the seconds to wait for the install lock, the minutes after
# which a crashed upload's copy is swept, the seconds the backend gives the
# whole run, then each platform's pin as three words: its name, its SHA-256
# and its size.
#
# It names the machine's platform first ("platform <name>"), then prints
# "ready <path>", "missing <dir>" (only looking, and no ready copy) or
# "upload <dir>". A copy whose marker and size match is ready with no lock
# and no hash. Otherwise, under the install lock: a copy the right size is
# hashed and kept if it matches, else replaced by a download verified in a
# temp file beside it, renamed in only once its hash matches. With no curl,
# or a download that fails or does not match, it asks for the backend's copy,
# which a second run given its path verifies and installs the same way. A
# copy that matches but cannot be put in place fails as that (exit 74),
# never as a mismatch, and asks for no copy. A Linux on musl, which the
# Linux builds do not run on, exits 64 before anything is looked at or
# fetched; a platform with no build at all exits 69.
#
# It runs on macOS and BusyBox as well as GNU: options go before the first
# operand (BSD tools read a `--` after one as a file), and no long options.
set -u
umask 077
c=$1; v=$2; r=$3; u=$4; install=$5; lock_wait_s=$6; upload_stale_min=$7; run_s=$8
shift 8
k=$(uname -s); a=$(uname -m)
if [ "$k" = Darwin ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" = 1 ]; then a=arm64; fi
case "$a" in x86_64|amd64) a=x64 ;; aarch64|arm64) a=arm64 ;; esac
case "$k" in
  Darwin) p=darwin-$a ;;
  Linux)
    p=linux-$a
    if is_musl; then exit 64; fi ;;
  *) p= ;;
esac
s=; n=
while [ $# -ge 3 ]; do
  if [ "$1" = "$p" ]; then s=$2; n=$3; fi
  shift 3
done
if [ -z "$s" ]; then echo "Claude Code has no build for $k on $(uname -m)." >&2; exit 69; fi
echo "platform $p"
d="$c/$v/$p"; b="$d/claude"; m="$b.verified"
size() { stat -c %s -- "$1" 2>/dev/null || stat -f %z -- "$1" 2>/dev/null; }
ready() { [ -f "$b" ] && [ ! -L "$b" ] && [ -x "$b" ] && [ "$(cat -- "$m" 2>/dev/null)" = "$s $n" ] && [ "$(size "$b")" = "$n" ]; }
if [ -z "$u" ] && ready; then touch -- "$m"; echo "ready $b"; exit 0; fi
if [ "$install" != 1 ]; then echo "missing $d"; exit 0; fi

command -v flock >/dev/null 2>&1 || command -v perl >/dev/null 2>&1 || { echo "This computer has neither flock nor perl, which installing Claude Code needs." >&2; exit 69; }
command -v sha256sum >/dev/null 2>&1 || command -v shasum >/dev/null 2>&1 || command -v perl >/dev/null 2>&1 || { echo "This computer has no sha256sum, shasum or perl to check Claude Code's download with." >&2; exit 69; }
mkdir -p -- "$d" && exec 9>>"$c/.lock" || { echo "Cannot write Claude Code's files under $c." >&2; exit 73; }
lock "$lock_wait_s" || { echo "Another install of Claude Code on this computer is still running." >&2; exit 75; }
sha() {
  if command -v sha256sum >/dev/null 2>&1; then h=$(sha256sum < "$1")
  elif command -v shasum >/dev/null 2>&1; then h=$(shasum -a 256 < "$1")
  else h=$(perl -MDigest::SHA -e 'print Digest::SHA->new(256)->addfile(shift, "b")->hexdigest' "$1"); fi
  [ "${h%% *}" = "$s" ]
}
good() { [ -f "$1" ] && [ ! -L "$1" ] && [ "$(size "$1")" = "$n" ] && sha "$1"; }
mark() { printf '%s\n' "$s $n" > "$m.new" && mv -f -- "$m.new" "$m"; }
place() { chmod 755 "$1" && mv -f -- "$1" "$b" && mark; }
unplaced() { [ -z "${1:-}" ] || rm -f -- "$1"; echo "Claude Code could not be set up at $d." >&2; exit 74; }
# Every version but this one and the newest other one used goes.
prune() {
  o=$(ls -td -- "$c"/*/*/claude.verified 2>/dev/null | while IFS= read -r x; do
    x=${x%/*/claude.verified}; [ "$x" = "$c/$v" ] || { printf '%s\n' "$x"; break; }
  done)
  for x in "$c"/*/; do x=${x%/}; [ "$x" = "$c/$v" ] || [ "$x" = "$o" ] || rm -rf -- "$x"; done
}
finish() { prune; echo "ready $b"; exit 0; }

if ready; then [ -z "$u" ] || rm -f -- "$u"; touch -- "$m"; finish; fi
rm -f -- "$m" "$d"/.download.*
find "$d" -name '.upload.*' -mmin +"$upload_stale_min" -exec rm -f -- {} + 2>/dev/null
if [ -n "$u" ]; then
  if good "$u"; then place "$u" && finish; unplaced "$u"; fi
  rm -f -- "$u"
  echo "The copy of Claude Code sent to this computer does not match its pinned SHA-256." >&2
  exit 66
fi
if good "$b"; then chmod 755 "$b" && mark && finish; unplaced; fi
rm -rf -- "$b"
if command -v curl >/dev/null 2>&1; then
  t=$(mktemp "$d/.download.XXXXXX") || exit 73
  # A download that stalls (under 1 KB/s for a minute) is cut, one still
  # moving runs until just before the run's own end, retries start only in
  # the first two minutes, and curl never holds the lock's descriptor, so a
  # channel cut never leaves it holding the lock.
  left=$((run_s - SECONDS - 150)); [ "$left" -gt 60 ] || left=60
  if curl -q -fsS --retry 2 --retry-max-time 120 --connect-timeout 20 --speed-limit 1024 --speed-time 60 \
    --max-time "$left" -o "$t" "$r/$v/$p/claude" 9>&-; then
    if good "$t"; then place "$t" && finish; unplaced "$t"; fi
    echo "The download of Claude Code does not match its pinned SHA-256." >&2
  fi
  rm -f -- "$t"
else
  echo "This computer has no curl." >&2
fi
echo "upload $d"
