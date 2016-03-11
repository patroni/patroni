#!/bin/bash

ETCDVERSION=2.2.5

BINDIR=bin
[ -d $BINDIR ] || mkdir $BINDIR

export PATH=$BINDIR:$PATH

# Add etcd
curl -L https://github.com/coreos/etcd/releases/download/v${ETCDVERSION}/etcd-v${ETCDVERSION}-linux-amd64.tar.gz | tar xz -C $BINDIR --strip=1 --wildcards --no-anchored etcd etcdctl

sudo pip2.7 install lettuce python-Levenshtein
sudo pip2.7 install -r requirements-py2.txt

exec lettuce
