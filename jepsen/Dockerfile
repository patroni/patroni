ARG PG_MAJOR=17
ARG PGHOME=/home/postgres
ARG DEBIAN_FRONTEND=noninteractive
ARG BRANCH=master

FROM postgres:$PG_MAJOR AS base

ARG PGHOME DEBIAN_FRONTEND
RUN echo 'APT::Install-Recommends "0";\n\
APT::Get::Assume-Yes "true";\n\
APT::Get::allow-downgrades "true";\n\
APT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01buildconfig && \
    apt-get update -y && \
    apt-get upgrade -y && \
    # allow starting sshd
    echo '#!/bin/sh' > /usr/sbin/policy-rc.d && \
    apt-get install -y etcd-client runit vim curl ca-certificates jq git less locales rsync \
        iptables bind9-host net-tools iputils-ping sudo telnet openssh-server coreutils && \
    # Make sure we have a en_US.UTF-8 locale available
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 && \
    # vim config
    echo 'syntax on\nfiletype plugin indent on\nset mouse-=a\nautocmd FileType yaml setlocal ts=2 sts=2 sw=2 expandtab' > /etc/vim/vimrc.local && \
    mkdir -p $PGHOME/archive && \
    mkdir -p $PGHOME/.ssh && \
    sed -i "s|/var/lib/postgresql.*|$PGHOME:/bin/bash|" /etc/passwd && \
    # jsch does not support rsa-sha2, we have to use the insecure ssh-rsa
    echo 'HostKeyAlgorithms +ssh-rsa\nPubkeyAcceptedKeyTypes +ssh-rsa' >> /etc/ssh/sshd_config && \
    # generate ssh key
    mkdir -p /root/.ssh && \
    chmod 700 /root/.ssh && \
    cd /root/.ssh && \
    yes | ssh-keygen -m PEM -t rsa -N '' -f id_rsa -C jepsen || true && \
    cp id_rsa.pub authorized_keys && \
    cp -a ./* $PGHOME/.ssh/ && \
    chown -R postgres:postgres $PGHOME

ENTRYPOINT ["/usr/bin/runsvdir", "-P", "/etc/service"]

FROM base AS etcd
RUN apt-get install -y etcd-server
COPY runit/etcd /etc/service/etcd


FROM base AS patroni

ARG BRANCH PGHOME PG_MAJOR DEBIAN_FRONTEND
ARG PGBIN=/usr/lib/postgresql/$PG_MAJOR/bin

RUN apt-cache depends patroni | sed -n -e 's/.* Depends: \(python3-.\+\)$/\1/p' \
            | grep -Ev '^python3-(sphinx|etcd|consul|kazoo|kubernetes|psycopg)' \
            | xargs apt-get install -y  python3-pip python3-wheel python3-setuptools python3-etcd \
    && pip3 install --break-system-packages "git+https://github.com/patroni/patroni.git@${BRANCH}#egg=patroni[psycopg3]" && \
    mkdir -p ~/.config/patroni && \
    ln -s /home/postgres/patroni.yml ~/.config/patroni/patronictl.yaml

COPY runit/patroni /etc/service/patroni

ENV PGHOME=$PGHOME PGDATA=$PGHOME/data PATH=$PATH:$PGBIN
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 EDITOR=/usr/bin/editor


FROM base AS jepsen

RUN apt-get install -y libjna-java default-jre-headless && \
    curl -sL https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > /usr/bin/lein && \
    chmod +x /usr/bin/lein

COPY jepsen /root/jepsen
WORKDIR /root/jepsen

ENV LEIN_ROOT=1
RUN lein install && lein deps
