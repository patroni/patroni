# syntax = docker/dockerfile:1.5
# Used only for running tests using tox, see ../tox.ini
ARG PG_MAJOR
ARG PGHOME=/home/postgres
ARG PGDATA=$PGHOME/data
ARG LC_ALL=C.UTF-8
ARG LANG=C.UTF-8
ARG NAME

FROM postgres:${PG_MAJOR}

ARG PGHOME
ARG PGDATA
ARG LC_ALL
ARG LANG
ARG NAME

ENV ETCDVERSION=3.3.13
# CONFDVERSION=0.16.0

ENV NAME=${NAME:-patroni-dev}

USER root
RUN set -ex \
    && apt-get update \
    && apt-get reinstall init-system-helpers \
    && apt-get install -y \
      python3-pip \
      python3-dev \
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
    && python3 -m pip install --no-cache-dir tox \
    \
    && mkdir -p "$PGHOME" \
    && sed -i "s|/var/lib/postgresql.*|$PGHOME:/bin/bash|" /etc/passwd \
    && chown -R postgres:postgres /var/log /home/postgres \
    \
    # Download etcd \
    && ETCDURL="https://github.com/coreos/etcd/releases/download/v$ETCDVERSION" \
    && curl -sL "$ETCDURL/etcd-v$ETCDVERSION-linux-$(dpkg --print-architecture).tar.gz" \
       | tar xz -C /usr/local/bin --strip=1 --wildcards --no-anchored etcd etcdctl

# For architectures such as aarch we need to get the respective GOARCH
# so we can tell etcd we're ok with running an unsupported architecture.
# This Dockerfile syntax only works with docker buildx and the syntax
# line at the top of this file.
COPY <<EOF /tox-wrapper.sh
#!/usr/bin/env bash
set -ex
export ETCD_UNSUPPORTED_ARCH=$(go env GOARCH)
exec tox "\$@"
EOF
RUN chmod +x /tox-wrapper.sh

USER postgres
ENV PATH=/venv/bin:$PATH

VOLUME /src

ENTRYPOINT ["sleep", "infinity"]
