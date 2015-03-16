# PostgreSQL HA with etcd

To get started, do the following from different terminals:

```
> etcd --data-dir=data/etcd
> run.py postgresql0.yml
> run.py postgresql1.yml
```

From there, you will see a high-availability cluster start up. Test
different settings in the YAML files to see how behavior changes.  Kill
some of the different components to see how the system behaves.

Cheers,
Chris
