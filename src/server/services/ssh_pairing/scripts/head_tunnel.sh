# the ports sshd listens on, its IP addresses, whether it has the OpenSSH
# client, and the id of the tunnel below if an earlier run set one up.
#
# When PRODUCT cannot reach the machine from outside, as behind a home or
# office router, the machine connects out instead. The script then checks
# that sshd answers here and keeps the rest in ~/TUNNEL_DIR: a key it
# makes for the tunnel, whose public half it sends, PRODUCT's gateway key,
# and a loop that holds an SSH connection open to that gateway, through which
# PRODUCT reaches this machine's sshd. The loop runs as a service of this
# account: a LaunchAgent on macOS; on Linux, a systemd user service, which it
# asks to keep running after logout, or else an @reboot line in the crontab.
# ~/TUNNEL_DIR/UNINSTALL_SCRIPT removes all of it.
#
# Nothing else is read or changed, and nothing needs root.
