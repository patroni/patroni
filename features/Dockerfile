# Used only for running tests using tox, see ../tox.ini
FROM patroni

WORKDIR $PGHOME
USER root
RUN apt-get update \
    && apt-get install -y python3-pip \
    && rm -rf /var/cache/apt \
    && python3 -m pip install --no-cache-dir tox \
    && chown -R postgres:postgres /home/postgres

USER postgres
ENV PATH=/venv/bin:$PATH

VOLUME /src

ENTRYPOINT ["sleep", "infinity"]
