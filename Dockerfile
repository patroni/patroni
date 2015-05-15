## This Dockerfile is meant to aid in the building and debugging governor whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM zalando/ubuntu:14.04.1-1
MAINTAINER Feike Steenbergen <feike.steenbergen@zalando.de>

# Add PGDG repositories
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RUN apt-get update -y
RUN apt-get upgrade -y

ENV PGVERSION 9.4
RUN apt-get install python python-psycopg2 python-yaml python-requests postgresql-${PGVERSION} -y

ENV PATH /usr/lib/postgresql/${PGVERSION}/bin:$PATH

RUN mkdir -p /governor/helpers
ADD governor.py /governor/governor.py
ADD helpers /governor/helpers
ADD postgres0.yml /governor/

ENV ETCDVERSION 2.0.10
RUN curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz | tar xz -C /bin --strip=1 --wildcards --no-anchored etcd etcdctl

## Setting up a simple script that will serve as an entrypoint
RUN mkdir /data/ && touch /var/log/etcd.log /var/log/etcd.err && chown postgres:postgres /var/log/etcd.*
RUN chown postgres:postgres -R /governor/ /data/
RUN /bin/echo -e "etcd --data-dir /tmp/etcd.data > /var/log/etcd.log 2> /var/log/etcd.err &\n/governor/governor.py /governor/postgres0.yml \"\$@\"" >> /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT /entrypoint.sh
USER postgres
