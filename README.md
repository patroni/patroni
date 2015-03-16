# Template for PostgreSQL HA with etcd

*There’s ways to to high availability with PostgreSQL; here we present a template for you to create your own custom fit high availability solution using etcd and python for maximum accessibility.*

To get started, do the following from different terminals:

```
> etcd --data-dir=data/etcd
> run.py postgresql0.yml
> run.py postgresql1.yml
```

From there, you will see a high-availability cluster start up. Test
different settings in the YAML files to see how behavior changes.  Kill
some of the different components to see how the system behaves.

# Requirements on a Mac

Run the following on a Mac to install requirements:

```
brew install postgresql etcd haproxy libyaml
pip install psycopg2 pyyaml
```

# Notice

There are many different ways to do HA with PostgreSQL, see [the
docs](https://wiki.postgresql.org/wiki/Replication,_Clustering,_and_Connection_Pooling) for a complete list.

We call this project a "template" because it is far from a one-size fits
all, or a plug-and-play replication system.  It will have it's own
caveats.  Use wisely.
