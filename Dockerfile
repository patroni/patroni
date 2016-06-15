## This Dockerfile is meant to aid in the building and debugging patroni whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM ubuntu:16.04
MAINTAINER Feike Steenbergen <feike.steenbergen@zalando.de>

# We need curl
RUN echo 'APT::Install-Recommends "0";' > /etc/apt/apt.conf.d/01norecommend
RUN echo 'APT::Install-Suggests "0";' >> /etc/apt/apt.conf.d/01norecommend

# Add PGDG repositories
ENV PGVERSION 9.5
RUN apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y curl postgresql-${PGVERSION} python-psycopg2 python-yaml python-requests python-six python-click \
         python-dateutil python-tzlocal python-urllib3 python-dnspython python-pip python-setuptools python-kazoo python \
    && pip install python-etcd==0.4.3 python-consul \
    && apt-get remove -y python-pip python-setuptools \
    && apt-get autoremove -y \
        # Clean up
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

ENV PATH /usr/lib/postgresql/${PGVERSION}/bin:$PATH

ADD patronictl.py patroni.py docker/entrypoint.sh /
ADD patroni /patroni/
RUN    ln -s /patroni/patroni.py /usr/local/bin/patroni \
    && ln -s /patroni/patronictl.py /usr/local/bin/patronictl

ENV ETCDVERSION 2.3.6
RUN curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz | tar xz -C /bin --strip=1 --wildcards --no-anchored etcd etcdctl

### Setting up a simple script that will serve as an entrypoint
RUN mkdir /data/ && touch /var/log/etcd.log /var/log/etcd.err /pgpass /patroni/postgres.yml \
    && chown postgres:postgres -R /patroni/ /data/ /pgpass /var/log/etcd.* /patroni/postgres.yml

EXPOSE 4001 5432 2380

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
USER postgres
