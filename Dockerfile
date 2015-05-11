## This Dockerfile is meant to aid in the building and debugging governor whilst developing on your local machine
## It has all the necessary components to play/debug with a single node appliance, running etcd
FROM ubuntu:14.04
MAINTAINER Feike Steenbergen <feike.steenbergen@zalando.de>

# Add PGDG repositories
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN apt-get install wget ca-certificates -y
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RUN apt-get update -y
RUN apt-get upgrade -y

ENV PGVERSION 9.4
RUN apt-get install curl python python-pip python-psycopg2 python-yaml postgresql-${PGVERSION} -y

RUN	ln -s /usr/lib/postgresql/* /usr/lib/postgresql/current
ENV PATH /usr/lib/postgresql/current/bin:$PATH

RUN mkdir -p /governor/helpers
ADD governor.py /governor/governor.py
ADD requirements.txt /governor/requirements.txt
ADD helpers /governor/helpers
ADD postgres0.yml /governor/
## As we are standalone, remove any reference to AWS
RUN sed -i '/aws_use_host_address/d' /governor/postgres0.yml

ENV ETCDVERSION 2.0.9
RUN curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz -o etcd-v${ETCDVERSION}-linux-amd64.tar.gz && tar vzxf etcd-v${ETCDVERSION}-linux-amd64.tar.gz && cp etcd-v${ETCDVERSION}-linux-amd64/etcd* /bin/

## Most requirements should already have been met, only as an extra precaution
RUN pip install -r /governor/requirements.txt

## Setting up a simple script that will serve as an entrypoint
RUN mkdir /data/ && touch /var/log/etcd.log /var/log/etcd.err && chown postgres:postgres /var/log/etcd.*
RUN chown postgres:postgres -R /governor/ /data/
RUN /bin/echo -e "etcd --data-dir /tmp/etcd.data > /var/log/etcd.log 2> /var/log/etcd.err &\n/governor/governor.py /governor/postgres0.yml \"$@\"" >> /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT /entrypoint.sh
USER postgres
