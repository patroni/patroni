.. _multisite:

Using Patroni in multisite mode
===============================

In some circumstances a tiered failover process is desirable where there are distinct sites, and failover across sites needs to be more resilient against temporary outages. For this purpose a special multisite mode is available. In multisite mode each site runs a separate Patroni cluster with its own DCS. To coordinate which site is the primary and which is the standby there is a global DCS for leader site election. In each site the local leader instance is responsible for global leader site election. The site that acquires the leader lock runs Patroni normally, other sites configure themselves as standby clusters.

When to use multisite mode
--------------------------

If network reliability and bandwidth between sites is good and latency low (<10ms), then most likely multisite mode is not useful. Instead a simple Patroni cluster that spans the two sites will be a simpler and more robust solution.

Multisite mode is useful when automatic cross site failover is needed, but the cross site failover needs to be much more resilient against temporary outages. It is also useful when cluster member IP addresses are not globally routable and cross site communication needs to pass through an externally visible proxy address.

Global DCS considerations
-------------------------

The multisite deployment will only be as resilient as the global DCS cluster. In case of a typical 3 node etcd cluster, this means that if any 2 nodes share a potential failure point, then that failure will bring the whole multisite cluster into read only mode within the multisite TTL timeout.

For example if there are 2 datacenters, and two of three etcd nodes are in datacenter A, then if the whole datacenter goes offline (e.g. power outage, fire, network connection to datacenter severed) then the other site will not be able to promote. If the other site happened to be leader at that point, it will demote itself to retain safety.

In short, this means that to survive a full site outage the system needs to have at least 3 sites. To simplify things, one of the 3 sites is only required to have a single etcd node. If only 2 sites are available, then hosting this third quorum node on public cloud infrastructure is a viable option.

Here is a typical deployment architecture for using multisite mode:

.. image:: _static/multisite-architecture.png

If the network latencies between sites are very high, then etcd might require special tuning. By default etcd uses a heartbeat interval of 100ms and election timeout of 1s. If round trip time between sites is more than 100ms then those values should be increased.

Configuration
-------------

Configuring multisite mode is done using a top level ``multisite`` section in Patroni configuration file.

- **multisite**:

  - **name**: Name for the site. All nodes that share the same value are considered to be a part of the same site.
  - **namespace**: Optional. Path within DCS where to store multisite state. Helpful to override in testing if the same etcd cluster is used for local and global DCS.
  - **etcd**: Same settings as when configuring main DCS.
  - **host**: Address that cluster members from the other sites can use to access this node when it is the leader.
  - **port**: Port used by other site members when connecting to this site.
  - **ttl**: Time to live of site leader lock. If the site is unable to elect a functioning leader within this timeout then a different site can take over the leader role.
  - **retry_timeout**: How long can the global etcd cluster be inaccessible before the cluster is demoted.

