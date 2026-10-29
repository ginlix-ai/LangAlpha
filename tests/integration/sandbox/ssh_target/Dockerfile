# A disposable machine for the SSH computer provider: Ubuntu 24.04 with
# OpenSSH, public-key login only, and no Python, so the provider's own
# bootstrap is what puts one there.
#
# Users:
#   dev     passwordless sudo, so exec_as_root works
#   nosudo  no sudo, so exec_as_root is refused
#
# Build and run (the port stays on loopback):
#
#   docker build -t langalpha-ssh-target tests/integration/sandbox/ssh_target
#   docker run -d --name langalpha-ssh-target -p 127.0.0.1:2222:22 \
#       -e AUTHORIZED_KEYS="$(cat client_key.pub)" langalpha-ssh-target
#
# AUTHORIZED_KEYS (one or more public key lines) is installed for both users
# at start, each line once however often the container restarts. A key can also be added to a running container:
#
#   docker exec -i langalpha-ssh-target \
#       sh -c 'cat >> /home/dev/.ssh/authorized_keys' < client_key.pub
#
# Then point the integration suite at it:
#
#   SANDBOX_TEST_PROVIDERS=ssh SSH_TEST_HOST=127.0.0.1 SSH_TEST_PORT=2222 \
#   SSH_TEST_USER=dev SSH_TEST_KEY_FILE=client_key \
#   uv run pytest -m integration tests/integration/sandbox/contract -v
#
# The host key is generated at build time, so it stays the same across runs
# of one image; rebuilding the image changes it.
FROM ubuntu:24.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openssh-server sudo curl ca-certificates procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd

RUN useradd -m -s /bin/bash dev \
    && useradd -m -s /bin/bash nosudo \
    && echo 'dev ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/dev \
    && chmod 0440 /etc/sudoers.d/dev \
    && for u in dev nosudo; do \
        install -d -m 0700 -o "$u" -g "$u" "/home/$u/.ssh"; \
        install -m 0600 -o "$u" -g "$u" /dev/null "/home/$u/.ssh/authorized_keys"; \
    done

RUN printf '%s\n' \
        'PasswordAuthentication no' \
        'KbdInteractiveAuthentication no' \
        'PermitRootLogin no' \
        'PubkeyAuthentication yes' \
        > /etc/ssh/sshd_config.d/10-pubkey-only.conf

RUN printf '%s\n' \
        '#!/bin/sh' \
        'set -e' \
        'printf "%s\n" "$AUTHORIZED_KEYS" | while IFS= read -r k; do' \
        '  [ -n "$k" ] || continue' \
        '  for u in dev nosudo; do' \
        '    f="/home/$u/.ssh/authorized_keys"' \
        '    grep -qxF "$k" "$f" || printf "%s\n" "$k" >> "$f"' \
        '  done' \
        'done' \
        'exec /usr/sbin/sshd -D -e' \
        > /usr/local/bin/entrypoint.sh \
    && chmod 0755 /usr/local/bin/entrypoint.sh

EXPOSE 22
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
