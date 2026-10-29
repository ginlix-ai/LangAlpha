"""Migration 084: tunnels for SSH machines the server cannot dial.

A machine behind a home router or a firewall cannot be reached at any address
of its own, so it connects out instead: it keeps an SSH connection open to the
deployment's gateway, asking it to forward one port back to its own sshd. The
server then reaches the machine through the gateway, still logging in to the
machine's own sshd with the owner's client key and the pinned host key.

``ssh_tunnels`` holds one row per machine account that connects this way.
``public_key`` is the key the machine made for its tunnel, which the gateway
accepts for the login named by ``tunnel_id`` and for nothing else, so one
account at most holds a key. Pairing the same machine again finds its row by
``fingerprint``, and moves ``paired_at``, as a pairing or a computer made
through the tunnel does. The gateway writes ``connected_at`` when the forward
comes up, ``disconnected_at`` when it goes down, and ``seen_at`` every minute in
between, so a server can tell whether the machine is there without dialing it.

``username``, ``host_key`` and ``port`` are the login the server last
reached the machine with through the tunnel. A tunnel no computer uses any
more, or never did, is taken off its machine by a sweep logged in that way,
since the service the pairing installed there would otherwise run for good;
``swept_at`` is the sweep's last try, so the workers take turns, stamped
again just before an uninstall runs, so no pairing renews the tunnel then.

``ssh_gateway_keys`` holds the gateway's own keys, made by whichever process
asks first: ``host``, which machines and servers pin, and ``dialer``, which the
server's workers log in to the gateway with. Each ``private_key`` is
``pgp_sym_encrypt`` of the OpenSSH private key text, under the key the vault
uses, as ``user_ssh_keys`` does.

Rollback: ``alembic downgrade 083``. It drops both tables, so every computer
reached through a tunnel stops answering until it is paired again, and the
gateway makes new keys that every installed tunnel refuses.
"""

from alembic import op

revision = "084"
down_revision = "083"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS ssh_tunnels (
            tunnel_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL,
            public_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            hostname TEXT NOT NULL DEFAULT '',
            username TEXT NOT NULL DEFAULT '',
            host_key TEXT NOT NULL DEFAULT '',
            port INTEGER NOT NULL DEFAULT 22,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            paired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            connected_at TIMESTAMPTZ,
            disconnected_at TIMESTAMPTZ,
            seen_at TIMESTAMPTZ,
            swept_at TIMESTAMPTZ,
            UNIQUE (fingerprint)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS ssh_gateway_keys (
            name VARCHAR(32) PRIMARY KEY,
            public_key TEXT NOT NULL,
            private_key BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ssh_gateway_keys")
    op.execute("DROP TABLE IF EXISTS ssh_tunnels")
