### confd

`confd` directory contains haproxy and pgbouncer template files for the [confd](https://github.com/kelseyhightower/confd) -- lightweight configuration management tool
You need to copy content of `confd` directory into /etcd/confd and run confd service:
```bash
$ confd -prefix=/service/$PATRONI_SCOPE -backend etcd -node $PATRONI_ETCD_URL -interval=10
```
It will periodically update haproxy.cfg and pgbouncer.ini with the actual list of Patroni nodes from `etcd` and "reload" haproxy and pgbouncer.ini when it is necessary.


### remco

`remco` directory contains haproxy and pgbouncer template files for
[remco](https://github.com/HeavyHorst/remco) -- 
a lightweight configuration management tool, similar to confd but with pongo2 templates and support for multiple backends per resource.

Copy the content of `remco` into `/etc/remco` (`resource.d` -> `/etc/remco/resource.d`,
`template` -> `/etc/remco/templates`), set your etcd nodes and scope in the `resource.d/*.toml` files, then run remco:

```bash
$ remco -config /etc/remco/config
```

It will watch `/members` (and `/leader` for pgbouncer) under `/service/$PATRONI_SCOPE` in etcd, re-render `haproxy.cfg` / `pgbouncer.ini`
whenever the Patroni cluster topology changes, and reload haproxy / pgbouncer
accordingly.


### startup-scripts

`startup-scripts` directory contains startup scripts for various OSes and management tools for Patroni.
