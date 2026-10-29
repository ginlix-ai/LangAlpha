"""The tunnel gateway, for machines the server cannot dial.

A machine behind a home router or a firewall connects out instead: it runs
``ssh -N -R langalpha:22:localhost:<sshd port> <tunnel id>@<gateway>`` and
keeps that connection open. A worker logs in as ``dial`` and opens a
direct-tcpip channel to the tunnel id; the gateway answers it with a
forwarded-tcpip channel on the machine's connection, and asyncssh relays the
bytes between the two. The worker's SSH session runs end to end with the
machine's own sshd inside that channel, so the gateway carries bytes it cannot
read, and the worker still checks the machine's pinned host key and logs in
with the owner's key.

Run it with ``python -m src.server.ssh_gateway``, one instance per database:
it is the only writer of a tunnel's liveness, so a second one finds the first
holding the database's gateway lock and exits. Once bound, it marks every
tunnel down, since none of a previous run's connections outlive it.
"""
