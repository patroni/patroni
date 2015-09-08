## This Dockerfile is meant to aid in the building and debugging patroni whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM ubuntu:14.04
MAINTAINER Feike Steenbergen <feike.steenbergen@zalando.de>

# We need curl
RUN apt-get update -y && apt-get install curl -y

# Add PGDG repositories
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update -y
RUN apt-get upgrade -y

ENV PGVERSION 9.4
RUN apt-get install python python-psycopg2 python-yaml python-requests python-boto postgresql-${PGVERSION} python-dnspython python-kazoo python-pip -y
RUN pip install python-etcd

ENV PATH /usr/lib/postgresql/${PGVERSION}/bin:$PATH

ADD patroni.py /patroni.py
ADD patroni/ /patroni

ENV ETCDVERSION 2.0.13
RUN curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz | tar xz -C /bin --strip=1 --wildcards --no-anchored etcd etcdctl

### Setting up a simple script that will serve as an entrypoint
RUN mkdir /data/ && touch /var/log/etcd.log /var/log/etcd.err /pgpass /patroni/postgres.yml
RUN chown postgres:postgres -R /patroni/ /data/ /pgpass /var/log/etcd.* /patroni/postgres.yml
ADD docker/entrypoint.sh /entrypoint.sh

EXPOSE 4001 5432 2380

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
USER postgres
