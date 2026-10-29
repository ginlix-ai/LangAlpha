
# Curl config, as $report is.
tunnel=TUNNEL_URL
d="$HOME/TUNNEL_DIR"
lock="$d/setup.lock"
held=

# Writes a file whole under this run's own name and moves it into place, so
# whoever reads it, a running loop or a run beside this one, finds the old
# file or the new one and never half of either.
put() {
  printf '%s\n' "$2" > "$1.$$" && mv -f "$1.$$" "$1" || fail "Could not write $1."
}

# One run at a time sets the tunnel up, so a second one started beside it
# waits, then finds the first one's key and with it the same tunnel. mkdir is
# the lock because it is atomic everywhere, and macOS has no flock.
lock_setup() {
  waited=0
  until mkdir "$lock" 2>/dev/null; do
    owner=$(cat "$lock/pid" 2>/dev/null)
    # A run killed while holding it leaves it behind: its pid is gone, or the
    # lock is older than any setup takes, should the pid have been reused.
    if { [ -n "$owner" ] && ! kill -0 "$owner" 2>/dev/null; } \
      || [ -n "$(find "$lock" -maxdepth 0 -mmin +5 2>/dev/null)" ]; then
      rm -rf "$lock"
      continue
    fi
    [ "$waited" -gt 0 ] || echo "Another run of the command is setting up the tunnel. Waiting for it..."
    [ "$waited" -lt 30 ] \
      || fail "Another run of the command is still setting up the tunnel. Wait for it to finish, then run the command again."
    waited=$((waited + 1))
    sleep 1
  done
  held=1
  trap unlock_setup EXIT
  echo $$ > "$lock/pid"
}

unlock_setup() {
  [ -z "$held" ] || rm -rf "$lock"
  held=
}

# Whether the loop in place is the one this run would start, put there moments
# ago by a run beside this one, and running with its ssh up. That run may be
# reaching the machine through it right now, so it is left alone. Any other,
# one waiting out a failure or one an older run left, starts over, which is
# what running the command again is for.
just_started() {
  cmp -s "$d/run.sh.$$" "$d/run.sh" \
    && [ -n "$(find "$d/run.sh" -mmin -2 2>/dev/null)" ] \
    && ps -eo args= 2>/dev/null | D="$d" awk '
      { sub(/^[ \t]+/, "") }
      $0 == "/bin/sh " ENVIRON["D"] "/run.sh" { loop = 1 }
      index($0, "IdentityFile=" ENVIRON["D"] "/id_ed25519") { ssh = 1 }
      END { exit !(loop && ssh) }'
}

# Removes the tunnel's service, its loop and its folder; with --keep-files,
# only the service and the loop, for a later run that replaces them.
write_uninstall() {
  {
    cat <<'EOF'
#!/bin/sh
# Takes this machine's PRODUCT tunnel away: the service that runs it, the loop
# holding it open, and this folder. Safe to run again, and over the tunnel
# itself, which drops the moment the loop stops, so the files go first.
EOF
    printf "d='%s'\n" "$d"
    cat <<'EOF'
label=ai.langalpha.TUNNEL_NAME
plist="$HOME/Library/LaunchAgents/$label.plist"
unit=langalpha-TUNNEL_NAME.service
unit_file="$HOME/.config/systemd/user/$unit"

if command -v crontab >/dev/null 2>&1 && crontab -l 2>/dev/null | grep -qF "$d/run.sh"; then
  crontab -l 2>/dev/null | grep -vF "$d/run.sh" | crontab -
fi
[ "${1:-}" = --keep-files ] || rm -rf "$d"
if command -v launchctl >/dev/null 2>&1; then
  launchctl bootout "gui/$(id -u)/$label" 2>/dev/null \
    || launchctl unload "$plist" 2>/dev/null
  rm -f "$plist"
fi
if [ -f "$unit_file" ]; then
  systemctl --user disable --now "$unit" >/dev/null 2>&1
  rm -f "$unit_file"
  systemctl --user daemon-reload >/dev/null 2>&1
fi
# The loop, by the exact command every service starts it with, and its ssh, by
# the key it logs in with: an editor or a pager open on run.sh is left alone.
pids=$(ps -eo pid=,args= 2>/dev/null | D="$d" awk '{
  pid = $1
  sub(/^[ \t]*[0-9]+[ \t]+/, "")
  if ($0 == "/bin/sh " ENVIRON["D"] "/run.sh" ||
      index($0, "IdentityFile=" ENVIRON["D"] "/id_ed25519")) print pid
}')
[ -z "$pids" ] || kill $pids 2>/dev/null
exit 0
EOF
  } > "$d/UNINSTALL_SCRIPT.$$" && mv -f "$d/UNINSTALL_SCRIPT.$$" "$d/UNINSTALL_SCRIPT" \
    || fail "Could not write $d/UNINSTALL_SCRIPT."
}

# Written beside the one in place, for connect_out to compare and move over
# it: a loop still running reads its script as it goes, and would read a file
# rewritten in place as garbage.
write_run() {
  {
    cat <<'EOF'
#!/bin/sh
# Holds this machine's tunnel to PRODUCT open, connecting again whenever it
# drops. PRODUCT reaches this machine's own sshd through it and logs in there
# as any SSH client would. Run by this account's service; UNINSTALL_SCRIPT
# beside it takes it all away.
EOF
    printf "d='%s'\ngateway='%s'\nport='%s'\nlogin='%s'\nsshd_port='%s'\n" \
      "$d" "$gateway" "$gateway_port" "$tid" "$sshd_port"
    cat <<'EOF'
log="$d/tunnel.log"
child=
# A stop takes down whatever runs at the moment, ssh or the wait between tries.
trap 'kill "$child" 2>/dev/null; exit 0' TERM INT
# Each wait is drawn between half and one and a half times its step, so the
# machines a gateway restart drops together come back spread out rather than
# all at once. POSIX sh has no $RANDOM: the seed mixes /dev/urandom, where od
# can read it, this tunnel's id and this loop's pid, and each draw steps it on.
seed=$(od -An -N2 -tu2 /dev/urandom 2>/dev/null | tr -cd 0-9)
crc=$(printf %s "$login" | cksum | awk '{ print $1 % 65536 }')
seed=$(( (${seed:-0} + $$ + ${crc:-0}) % 65536 ))
draw_pause() {
  seed=$(( (seed + 1) * 75 % 65537 - 1 ))
  pause=$(( $1 / 2 + seed % ($1 + 1) ))
}
delay=5
while [ -d "$d" ]; do
  started=$(date +%s)
  date > "$log"
  ssh -F /dev/null -N -T -o BatchMode=yes -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o ConnectTimeout=15 \
    -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$d/known_hosts" \
    -o CheckHostIP=no -o IdentitiesOnly=yes -o IdentityFile="$d/id_ed25519" \
    -o IdentityAgent=none -p "$port" -R "langalpha:22:localhost:$sshd_port" \
    "$login@$gateway" 2>> "$log" &
  child=$!
  wait "$child"
  # A connection that held for a minute starts the waits over.
  [ $(($(date +%s) - started)) -lt 60 ] || delay=5
  if grep -q 'Permission denied' "$log"; then
    # Most likely this tunnel was removed. Asked again now and then rather
    # than never, in case it was refused by mistake.
    draw_pause 600
  else
    draw_pause "$delay"
    delay=$((delay * 2))
    [ "$delay" -le 30 ] || delay=30
  fi
  sleep "$pause" &
  child=$!
  wait "$child"
done
EOF
  } > "$d/run.sh.$$" || fail "Could not write $d/run.sh."
}

# Each service file is written under a hidden name of this run's own and
# moved into place, so launchd or systemd never reads one half written.
start_service() {
  if [ "$os" = Darwin ]; then
    agents="$HOME/Library/LaunchAgents"
    plist="$agents/ai.langalpha.TUNNEL_NAME.plist"
    new="$agents/.ai.langalpha.TUNNEL_NAME.$$"
    mkdir -p "$agents" || fail "Could not create $agents."
    cat > "$new" <<EOF && mv -f "$new" "$plist" || fail "Could not write $plist."
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>ai.langalpha.TUNNEL_NAME</string>
  <key>ProgramArguments</key>
  <array><string>/bin/sh</string><string>$d/run.sh</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$d/service.log</string>
  <key>StandardErrorPath</key><string>$d/service.log</string>
</dict>
</plist>
EOF
    # The agent an earlier run left may still be on its way out.
    tries=0
    until launchctl bootstrap "gui/$(id -u)" "$plist" 2>/dev/null; do
      tries=$((tries + 1))
      if [ "$tries" -ge 5 ]; then
        launchctl load -w "$plist" 2>/dev/null || fail "Could not start the tunnel with launchctl."
        break
      fi
      sleep 1
    done
  elif command -v systemctl >/dev/null 2>&1 && systemctl --user show-environment >/dev/null 2>&1; then
    units="$HOME/.config/systemd/user"
    unit="$units/langalpha-TUNNEL_NAME.service"
    new="$units/.langalpha-TUNNEL_NAME.$$"
    mkdir -p "$units" || fail "Could not create $units."
    cat > "$new" <<EOF && mv -f "$new" "$unit" || fail "Could not write $unit."
[Unit]
Description=PRODUCT tunnel

[Service]
ExecStart=/bin/sh $d/run.sh
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload \
      && systemctl --user enable --now langalpha-TUNNEL_NAME.service >/dev/null 2>&1 \
      || fail "Could not start the tunnel with systemctl --user."
    # Without lingering, systemd stops an account's services at its last logout.
    loginctl --no-ask-password enable-linger "$user" </dev/null >/dev/null 2>&1 \
      || loginctl show-user "$user" -p Linger 2>/dev/null | grep -qx Linger=yes \
      || echo "The tunnel runs only while $user is logged in: this account may not keep services running after it logs out."
  else
    if command -v crontab >/dev/null 2>&1; then
      { crontab -l 2>/dev/null; echo "@reboot /bin/sh $d/run.sh >/dev/null 2>&1"; } | crontab - \
        || echo "Could not add the tunnel to this account's crontab, so it will not come back after a restart. Run the command again then."
    else
      echo "This machine has no crontab, so the tunnel will not come back after a restart. Run the command again then."
    fi
    nohup /bin/sh "$d/run.sh" </dev/null >/dev/null 2>&1 &
  fi
}

# Leaves this machine holding an SSH connection open to PRODUCT, which reaches
# the machine's own sshd through it. A later run keeps the tunnel's key, so it
# keeps the same tunnel, and the computers already on it keep working.
connect_out() {
  sshd_port=${ports%% *}
  [ -n "$sshd_port" ] || sshd_port=22
  if command -v ssh-keyscan >/dev/null 2>&1 \
    && [ -z "$(ssh-keyscan -T 5 -p "$sshd_port" localhost 2>/dev/null)" ]; then
    [ "$os" = Darwin ] && fail "Turn on Remote Login (System Settings, General, Sharing), then run the command again."
    fail "No SSH server answers on port $sshd_port of this machine. Start sshd, then run the command again."
  fi
  # The folder goes unquoted into a service definition and a crontab line.
  case "$d" in *[!A-Za-z0-9._/@+-]*) fail "The tunnel cannot be kept in $d, whose path has characters it cannot use." ;; esac
  mkdir -p "$d" && chmod 700 "$d" || fail "Could not create $d."
  lock_setup
  if [ ! -f "$d/id_ed25519" ]; then
    # Made under this run's own name, then linked into place, which fails once
    # the name is taken: should a run get past the lock, the first key in
    # place still wins and every run uses it.
    rm -f "$d/id_ed25519.$$" "$d/id_ed25519.$$.pub"
    ssh-keygen -q -t ed25519 -N '' -C langalpha-tunnel -f "$d/id_ed25519.$$" </dev/null >/dev/null \
      && { ln "$d/id_ed25519.$$" "$d/id_ed25519" 2>/dev/null || [ -f "$d/id_ed25519" ] \
        || mv "$d/id_ed25519.$$" "$d/id_ed25519"; } \
      || fail "Could not make a key for the tunnel."
    rm -f "$d/id_ed25519.$$" "$d/id_ed25519.$$.pub"
  fi
  # From the key in place, whatever an earlier run left beside it.
  pub=$(ssh-keygen -y -f "$d/id_ed25519" </dev/null 2>/dev/null) && [ -n "$pub" ] \
    || fail "Could not read the tunnel's key."
  put "$d/id_ed25519.pub" "$pub"

  out=$(printf '%s\n' "$tunnel" | curl -K - -sS --max-time 60 -w '\n%{http_code}' \
    --data-urlencode "public_key=$pub" \
    --data-urlencode "hostname=$name") || unreachable
  answer=$(printf '%s\n' "$out" | sed '$d')
  [ "$(printf '%s\n' "$out" | tail -n 1)" = 200 ] \
    || fail "${answer:-PRODUCT could not set up the tunnel. Run the command again.}"
  tid=$(printf '%s\n' "$answer" | sed -n 's/^tunnel_id=//p')
  gateway=$(printf '%s\n' "$answer" | sed -n 's/^gateway_host=//p')
  gateway_port=$(printf '%s\n' "$answer" | sed -n 's/^gateway_port=//p')
  known=$(printf '%s\n' "$answer" | sed -n 's/^known_hosts=//p')
  for v in "$tid" "$gateway" "$gateway_port"; do
    case "$v" in ''|*[!A-Za-z0-9.:-]*) fail "PRODUCT's answer could not be read. Run the command again." ;; esac
  done
  [ -n "$known" ] || fail "PRODUCT's answer could not be read. Run the command again."
  put "$d/known_hosts" "$known"
  put "$d/tunnel_id" "$tid"

  write_uninstall
  write_run
  if just_started; then
    rm -f "$d/run.sh.$$"
  else
    # One service at a time: whatever an earlier run started goes first.
    sh "$d/UNINSTALL_SCRIPT" --keep-files
    mv -f "$d/run.sh.$$" "$d/run.sh" || fail "Could not write $d/run.sh."
    start_service
  fi
  unlock_setup
  echo "The tunnel is running. To remove it: sh $d/UNINSTALL_SCRIPT"
}

echo "Asking PRODUCT to connect..."
set --
[ -r "$d/tunnel_id" ] && set -- --data-urlencode "tunnel_id=$(cat "$d/tunnel_id")"
command -v ssh >/dev/null 2>&1 && command -v ssh-keygen >/dev/null 2>&1 \
  && set -- ${1+"$@"} --data-urlencode can_tunnel=1
send ${1+"$@"}
if [ "$code" = 202 ] && [ "$(printf '%s\n' "$body" | sed -n 1p)" = TUNNEL_CUE ]; then
  printf '%s\n' "$body" | sed 1d
  connect_out
  echo "Asking PRODUCT to connect through the tunnel..."
  send --data-urlencode "tunnel_id=$tid" --data-urlencode can_tunnel=1
  printf '%s\n' "$body"
  [ "$code" = 200 ] && exit 0
  if [ -s "$d/tunnel.log" ]; then
    echo "The tunnel's last try, from $d/tunnel.log:"
    tail -n 5 "$d/tunnel.log"
  fi
  exit 1
fi
