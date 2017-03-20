## This Dockerfile is meant to aid in the building and debugging patroni whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM postgres:9.6
MAINTAINER Alexander Kukushkin <alexander.kukushkin@zalando.de>

RUN echo 'APT::Install-Recommends "0";\nAPT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01norecommend \
    && apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y curl jq haproxy python-psycopg2 python-yaml python-requests \
        python-six python-dateutil python-urllib3 python-dnspython \
        python-pip python-setuptools python-kazoo python-prettytable python-wheel python \

    && pip install python-etcd==0.4.3 python-consul==0.7.0 click tzlocal --upgrade \

    && mkdir -p /home/postgres \
    && chown postgres:postgres /home/postgres \

    # Clean up
    && apt-get remove -y python-pip python-setuptools \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

ENV ETCDVERSION 3.1.2
RUN curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz \
    | tar xz -C /usr/local/bin --strip=1 --wildcards --no-anchored etcd etcdctl

ENV CONFDVERSION 0.11.0
RUN curl -L https://github.com/kelseyhightower/confd/releases/download/v${CONFDVERSION}/confd-${CONFDVERSION}-linux-amd64 > /usr/local/bin/confd \
    && chmod +x /usr/local/bin/confd

ADD patronictl.py patroni.py docker/entrypoint.sh /
ADD patroni /patroni/
ADD extras/confd /etc/confd
RUN ln -s /patronictl.py /usr/local/bin/patronictl

### Setting up a simple script that will serve as an entrypoint
RUN mkdir /data/ && touch /pgpass /patroni.yml \
    && chown postgres:postgres -R /patroni/ /data/ /pgpass /patroni.yml /etc/haproxy /var/run/ /var/lib/ /var/log/

EXPOSE 2379 5432 8008

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
USER postgres
