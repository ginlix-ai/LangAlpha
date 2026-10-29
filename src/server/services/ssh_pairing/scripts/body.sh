set -u
key=KEY
# Where to report, as a line of curl config. Its URL holds this command's
# token, so curl reads it on its stdin: any account on this machine can list
# a command's arguments.
report=REPORT
tell=
# Named in every report and stop, so a run of the command still going after
# a later one checked in cannot stop that later one.
run=$(od -An -N8 -tx1 /dev/urandom 2>/dev/null | tr -d ' \n')
[ -n "$run" ] || run=$$

# Says why this run stopped. Once PRODUCT can be told, it is told too, so the
# dialog waiting on this run shows the same sentence rather than waiting on
# until the command expires. A run stopped once PRODUCT had already reached
# the machine, as by a Ctrl-C while the last answer printed, did its job, and
# PRODUCT's answer says so instead.
fail() {
  trap - INT TERM HUP
  if [ -n "$tell" ] && out=$(printf '%s\n' "$report" | curl -K - -s --max-time 15 \
      -w '\n%{http_code}' --data-urlencode "failed=$*" --data-urlencode "run=$run") \
    && [ "$(printf '%s\n' "$out" | tail -n 1)" = 409 ]; then
    printf '%s\n' "$out" | sed '$d'
    exit 0
  fi
  printf '%s\n' "$*" >&2
  exit 1
}
# Nothing is told when PRODUCT is what could not be reached.
unreachable() {
  tell=
  fail "Could not reach PRODUCT from this machine. Check its internet connection, then run the command again."
}

command -v curl >/dev/null 2>&1 || fail "curl is needed to report back to PRODUCT."
tell=1
trap 'fail "This command was stopped before it finished. Run it again."' INT TERM HUP
os=$(uname -s)
case "$os" in Linux|Darwin) ;; *) fail "Only Linux and macOS machines can be connected for now." ;; esac
user=$(id -un) || fail "Could not tell which account this is."

umask 077
mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh" || fail "Could not create $HOME/.ssh."
ak="$HOME/.ssh/authorized_keys"
if ! grep -qxF "$key" "$ak" 2>/dev/null; then
  # A last line without its newline would swallow the key.
  if [ -s "$ak" ] && [ -n "$(tail -c 1 "$ak")" ]; then echo >> "$ak"; fi
  printf '%s\n' "$key" >> "$ak" || fail "Could not write $ak."
fi
chmod 600 "$ak"
echo "Authorized PRODUCT to log in as $user."

host_keys=$(cat /etc/ssh/ssh_host_*_key.pub 2>/dev/null)
ports=$(cat /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf 2>/dev/null \
  | awk 'tolower($1) == "port" && $2 ~ /^[0-9]+$/ && $2 > 0 && $2 < 65536 { print $2 }' \
  | tr '\n' ' ')
# An sshd keeping its keys elsewhere, as dropbear, a Homebrew sshd or a
# HostKey setting does, is asked for them over this machine's own loopback.
# Without them PRODUCT will not connect by itself: it could not tell this
# machine from anything else answering at its address.
if [ -z "$host_keys" ] && command -v ssh-keyscan >/dev/null 2>&1; then
  port=${ports%% *}
  host_keys=$(ssh-keyscan -T 5 -p "${port:-22}" 127.0.0.1 ::1 </dev/null 2>/dev/null \
    | awk '$1 !~ /^#/ && NF >= 3 { print $2, $3 }')
fi
addresses=$(hostname -I 2>/dev/null) || addresses=$(ip -o addr show scope global 2>/dev/null \
  | awk '{ sub(/\/.*/, "", $4); print $4 }' | tr '\n' ' ')
# macOS has neither.
[ -n "$addresses" ] || addresses=$(ifconfig 2>/dev/null \
  | awk '$1 == "inet" && $2 !~ /^127\./ { print $2 }' | tr '\n' ' ')
name=$(hostname 2>/dev/null || uname -n)

# Sends the report, with any further fields given, and keeps the answer.
send() {
  out=$(printf '%s\n' "$report" | curl -K - -sS --max-time 120 -w '\n%{http_code}' \
    --data-urlencode "user=$user" \
    --data-urlencode "os=$os" \
    --data-urlencode "hostname=$name" \
    --data-urlencode "host_keys=$host_keys" \
    --data-urlencode "ports=$ports" \
    --data-urlencode "addresses=$addresses" \
    --data-urlencode "run=$run" \
    ${1+"$@"}) || unreachable
  code=$(printf '%s\n' "$out" | tail -n 1)
  body=$(printf '%s\n' "$out" | sed '$d')
}
