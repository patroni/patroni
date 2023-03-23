# syntax = docker/dockerfile:1.5
# Used only for running tests using tox, see ../tox.ini
ARG PG_MAJOR
FROM patroni:${PG_MAJOR}

USER root
RUN apt-get update \
    && apt-get reinstall init-system-helpers \
    && apt-get install -y \
      python3-pip \
      rsync \
      gcc \
      python3-dev \
      golang \
    && rm -rf /var/cache/apt \
    && python3 -m pip install --no-cache-dir tox \
    && chown -R postgres:postgres /home/postgres

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
