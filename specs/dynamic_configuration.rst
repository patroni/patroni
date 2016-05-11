Patroni configuration reload
============================

Patroni configuration should be stored in the DCS. There will be 3 types of configuration:

- bootstrap configuraton set in Patroni
	That should be applied during the initialization time and written to etcd.

- startup configuration (also in patroni.yml).
	They should be applied during the initialization time. Unlike other options, they are not written in etcd and
	any attempts to change them dynamically are blocked.

- dynamic configuration.
	Those options can be set in etcd at any time. If the options changed are not part of the startup configuration,
	they are applied asynchronously (upon the next wake up cycle) to every node, which gets subsequently reloaded.
	If the node requires a restart to apply the configuration (for options with context postmaster or internal, if
	their values have changed), a special flag indiciating this should be set in the members.data JSON. A new API
	endpoint should return whether the given node requires a restart. Additionally, the node status shoudl also 
	idincate this.

Some options may not be increased on the master independently of the replicas (master-dependent options):

- max_connections
- max_locks_per_transactions
- max_worker_processes
- max_prepared_transactions

Regarding the options that can be set, the following restrictions apply:

- dynamic configuration options that are also listed in the startup configuration will not be changed,
  except for the case of master-dependent options.

When applying the startup or dynamic configuration options, the following actions should be taken:

- The node should first check if there is a postgresql.conf.patroni.
- If it exists, it contains the renamed "original" configuration.
- If it doesn't, the original postgresql.conf is taken and renamed to postgresql.conf.patroni.
- The dynamic options (with the exceptions above) are dumped into the postgresql.conf and an include is set in
  postgresql.conf to postgresql.conf.patroni. Therefore, we would be able to apply new options without re-reading the configuration file to check if the include is present not.
- If some of the options that require restart are changed (we should look at the context in pg_settings and at the actual
  values of those options), a restart_pending flag of a given node should be set. This flag is reset on any restart.

Also, the following patroni configuration options can be changed dynamically:

- ttl
- loop_wait
- retry_timeouts (to be defined first in patroni.yaml)

Upon changing those options, Patroni should read the relevant section of the configuration storedi in DCS and change their
run-time values.

Patroni nodes should dump the state of the DCS options to disk on startup and upon every change of the configuration.
Only master is allowed to restore those options from the on-disk dump if those are completely absent from the DCS or invalid.








