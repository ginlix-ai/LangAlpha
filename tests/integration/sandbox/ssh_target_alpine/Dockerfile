# A BusyBox machine for the SSH computer provider: Alpine with OpenSSH,
# public-key login only, as ssh_target/Dockerfile is for glibc. It carries what
# an SSH computer needs (bash, curl) and nothing else: no perl, so the
# installer's lock and hash fall to busybox's flock and sha256sum. It is a
# userland to test the scripts on, not a supported machine: the pairing probe
# refuses musl.
#
#   docker build -t langalpha-ssh-target-alpine tests/integration/sandbox/ssh_target_alpine
#   docker run -d -p 127.0.0.1:2223:22 -e AUTHORIZED_KEYS="$(cat client_key.pub)" langalpha-ssh-target-alpine
FROM alpine:3.22

RUN apk add --no-cache openssh-server bash curl \
    && ssh-keygen -A \
    && adduser -D -s /bin/bash dev \
    && sed -i 's/^dev:!/dev:*/' /etc/shadow \
    && install -d -m 0700 -o dev -g dev /home/dev/.ssh \
    && printf '%s\n' 'PasswordAuthentication no' 'KbdInteractiveAuthentication no' 'PermitRootLogin no' \
        >> /etc/ssh/sshd_config

RUN printf '%s\n' \
        '#!/bin/sh' \
        'set -e' \
        'printf "%s\n" "$AUTHORIZED_KEYS" > /home/dev/.ssh/authorized_keys' \
        'chown dev:dev /home/dev/.ssh/authorized_keys && chmod 600 /home/dev/.ssh/authorized_keys' \
        'exec /usr/sbin/sshd -D -e' \
        > /usr/local/bin/entrypoint.sh \
    && chmod 0755 /usr/local/bin/entrypoint.sh

EXPOSE 22
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
