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
    && rm -rf /var/cache/apt \
    && python3 -m pip install --no-cache-dir tox \
    && chown -R postgres:postgres /home/postgres

USER postgres
ENV PATH=/venv/bin:$PATH

VOLUME /src

ENTRYPOINT ["sleep", "infinity"]
