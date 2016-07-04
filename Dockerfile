## This Dockerfile is meant to aid in the building and debugging patroni whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM ubuntu:16.04
MAINTAINER Feike Steenbergen <feike.steenbergen@zalando.de>

RUN echo 'APT::Install-Recommends "0";' > /etc/apt/apt.conf.d/01norecommend \
    && echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/01norecommend

ENV PGVERSION 9.5
ENV PATH /usr/lib/postgresql/${PGVERSION}/bin:$PATH
RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y curl jq haproxy zookeeper postgresql-${PGVERSION} python-psycopg2 python-yaml \
        python-requests python-six python-click python-dateutil python-tzlocal python-urllib3 \
        python-dnspython python-pip python-setuptools python-kazoo python-prettytable python \
    && pip install python-etcd==0.4.3 python-consul==0.6.0 --upgrade \
    && apt-get remove -y python-pip python-setuptools \
    && apt-get autoremove -y \
        # Clean up
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

ENV ETCDVERSION 2.3.6
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
    && chown postgres:postgres -R /patroni/ /data/ /pgpass /patroni.yml /etc/haproxy /var/run/ /var/lib/ /var/log/ \
    && echo 1 > /etc/zookeeper/conf/myid

EXPOSE 2379 5432 8008

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
USER postgres
