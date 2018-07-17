## This Dockerfile is meant to aid in the building and debugging patroni whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM postgres:10
MAINTAINER Alexander Kukushkin <alexander.kukushkin@zalando.de>

RUN export DEBIAN_FRONTEND=noninteractive \
    && echo 'APT::Install-Recommends "0";\nAPT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01norecommend \
    && apt-get update -y \
    && apt-get upgrade -y \
    # postgres:10 is based on debian, which has patroni package. We will install all required dependencies
    && apt-get install -s patroni | sed -n -e '/^Inst patroni /d' -e 's/^Inst \([^ ]\+\) .*$/\1/p' \
            | xargs apt-get install -y curl jq haproxy locales python3-etcd python3-kazoo \

    ## Make sure we have a en_US.UTF-8 locale available
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \

    && mkdir -p /home/postgres \
    && chown postgres:postgres /home/postgres \

    # Clean up
    && apt-get purge -y libpython2.7-stdlib libpython2.7-minimal \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

ENV ETCDVERSION 3.0.17
RUN curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz \
    | tar xz -C /usr/local/bin --strip=1 --wildcards --no-anchored etcd etcdctl

ENV CONFDVERSION 0.16.0
RUN curl -L https://github.com/kelseyhightower/confd/releases/download/v${CONFDVERSION}/confd-${CONFDVERSION}-linux-amd64 > /usr/local/bin/confd \
    && chmod +x /usr/local/bin/confd

ADD patronictl.py patroni.py docker/entrypoint.sh /
ADD patroni /patroni/
ADD extras/confd /etc/confd

RUN sed -i 's/env python/&3/' patroni*.py && ln -s /patronictl.py /usr/local/bin/patronictl && mkdir /data/ /run/haproxy \
    && touch /pgpass /patroni.yml && chown postgres:postgres -R /patroni/ /data/ /pgpass /patroni.yml /etc/haproxy /var/run/ /var/lib/ /var/log/

EXPOSE 2379 5432 8008

ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
USER postgres
