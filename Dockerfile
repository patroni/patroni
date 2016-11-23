## This Dockerfile is meant to aid in the building and debugging patroni whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM ubuntu:16.04
MAINTAINER Alexander Kukushkin <alexander.kukushkin@zalando.de>

RUN echo 'APT::Install-Recommends "0";' > /etc/apt/apt.conf.d/01norecommend \
    && echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/01norecommend

ENV PGVERSION 9.6
ENV PATH /usr/lib/postgresql/${PGVERSION}/bin:$PATH
RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y curl jq haproxy zookeeper python-psycopg2 python-yaml python-requests \
        python-six python-click python-dateutil python-tzlocal python-urllib3 python-dnspython \
        python-pip python-setuptools python-kazoo python-prettytable python-wheel python \

    && export DISTRIB_CODENAME=$(sed -n 's/DISTRIB_CODENAME=//p' /etc/lsb-release) \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ ${DISTRIB_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && echo "deb-src http://apt.postgresql.org/pub/repos/apt/ ${DISTRIB_CODENAME}-pgdg main" >> /etc/apt/sources.list.d/pgdg.list \
    && curl -s -o - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \

    ## Make sure we have a en_US.UTF-8 locale available
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \

    && apt-get update -y \
    && apt-get install -y postgresql-contrib-${PGVERSION} \

    # Remove the default cluster, which Debian stupidly starts right after installation of the packages
    && pg_dropcluster --stop ${PGVERSION} main \
    && pip install python-etcd==0.4.3 python-consul==0.6.0 --upgrade \

    # Clean up
    && apt-get remove -y python-pip python-setuptools \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

ENV ETCDVERSION 3.0.15
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
