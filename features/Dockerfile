# syntax = docker/dockerfile:1.5
# Used only for running tests using tox, see ../tox.ini
ARG PG_MAJOR
ARG PGHOME=/home/postgres
ARG LC_ALL=C.UTF-8
ARG LANG=C.UTF-8
ARG BASE_IMAGE=postgres

FROM ${BASE_IMAGE}:${PG_MAJOR}

ARG PGHOME
ARG LC_ALL
ARG LANG

ENV PGHOME="$PGHOME"
ENV PG_USER="${PG_USER:-postgres}"
ENV PG_GROUP="${PG_GROUP:-$PG_USER}"
ENV LC_ALL="$LC_ALL"
ENV LANG="$LANG"

ARG ETCDVERSION=3.3.13
ENV ETCDVERSION="$ETCDVERSION"
ARG ETCDURL="https://github.com/coreos/etcd/releases/download/v$ETCDVERSION"

USER root
RUN set -ex \
    && apt-get update \
    && apt-get reinstall init-system-helpers \
    && apt-get install -y \
      python3-dev \
      python3-venv \
      rsync \
      curl \
      gcc \
      golang \
      jq  \
      locales  \
      sudo \
      busybox \
      net-tools \
      iputils-ping \
    && rm -rf /var/cache/apt \
    \
    && python3 -m venv /tox \
    && /tox/bin/pip install --no-cache-dir tox>=4 \
    \
    && mkdir -p "$PGHOME" \
    && sed -i "s|/var/lib/postgresql.*|$PGHOME:/bin/bash|" /etc/passwd \
    && chown -R "$PG_USER:$PG_GROUP" /var/log /home/postgres \
    \
    # Download etcd \
    && curl -sL "$ETCDURL/etcd-v$ETCDVERSION-linux-$(dpkg --print-architecture).tar.gz" \
       | tar xz -C /usr/local/bin --strip=1 --wildcards --no-anchored etcd etcdctl

ENV PATH="/tox/bin:$PATH"

# This Dockerfile syntax only works with docker buildx and the syntax
# line at the top of this file.
COPY <<EOF /tox-wrapper.sh
#!/usr/bin/env bash
set -ex
copy_output() {
    if [[ -d "\$PGHOME/src/features/output" && /src/features ]] ;then
        cp -a "\$PGHOME/src/features/output" "/src/features/output-\$HOSTNAME"
        find "/src/features/output-\$HOSTNAME" -type f -exec chmod 666 {} \\;
        find "/src/features/output-\$HOSTNAME" -type d -exec chmod 777 {} \\;
    fi
}

# Ensure the copy is ran if the container is stopped with `docker stop` or `docker kill`
trap 'copy_output' SIGTERM
# For architectures such as aarch we need to get the respective GOARCH
# so we can tell etcd we're ok with running an unsupported architecture.
export ETCD_UNSUPPORTED_ARCH=$(go env GOARCH)
cd /src
runuser -u "\$PG_USER" -- \\
    find . ! -readable 2>/dev/null \\
    | sed 's|^./||' >/tmp/copy_exclude.lst \\
    || true
runuser -u "\$PG_USER" -- \\
    rsync -a \\
    --exclude=.tox \\
    --exclude="features/output*" \\
    --exclude-from="/tmp/copy_exclude.lst" \\
    . "\$PGHOME/src/"
cd "\$PGHOME/src"
runuser -u "\$PG_USER" -w ETCD_UNSUPPORTED_ARCH -- "\$@" &
wait $!
# SIGINT whilst child proc is running is not seen by trap so we run a copy here instead of using
# trap copy_output SIGINT EXIT
copy_output
EOF
RUN chmod +x /tox-wrapper.sh

VOLUME /src

ENTRYPOINT ["/tox-wrapper.sh"]
