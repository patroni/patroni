.. _watchdog:

================
Watchdog support
================

Having multiple PostgreSQL servers running as master can result in transactions lost due to diverging timelines. This situation is also called a split-brain problem. To avoid split-brain Patroni needs to ensure PostgreSQL will not accept any transaction commits after leader key expires in the DCS. Under normal circumstances Patroni will try to achieve this by stopping PostgreSQL when leader lock update fails for any reason. However, this may fail to happen due to various reasons:

- Patroni has crashed due to a bug, out-of-memory condition or by being accidentally killed by a system administrator.

- Shutting down PostgreSQL is too slow.

- Patroni does not get to run due to high load on the system, th VM being paused by the hypervisor, or other infrastructure issues.

To guarantee correct behavior under these conditions Patroni supports watchdog devices. Watchdog devices are software or hardware mechanisms that will reset the whole system when they do not get a keepalive heartbeat within a specified timeframe.

To be safe under all circumstances Patroni will set up the watchdog to expire after half of TTL. The watchdog will reset every time the high availability loop runs. This means that `ttl` must be at least twice `loop_wait` plus some safety margin. Default setup of `loop_wait=10` and `ttl=30` gives HA loop 5 seconds (ttl / 2 - loop_wait) to complete before the system gets forcefully reset. This is rather aggressive and you probably should increase `ttl` and/or reduce `loop_wait` if you decide to use a watchdog.

Currently watchdogs are only supported using Linux watchdog device interface.

Setting up software watchdog on Linux
-------------------------------------

Default Patroni configuration will try to use `/dev/watchdog` on Linux if it's accessible to Patroni. For most use cases using software watchdog built into the Linux kernel is secure enough.

To enable software watchdog issue the following commands as root before starting Patroni:

.. code-block:: bash

    modprobe softdog
    # Replace postgres with the user you will be running patroni under
    chown postgres /dev/watchdog

For testing it may be helpful to disable rebooting by adding `soft_noboot=1` to the modprobe command line. In this case the watchdog will just log a line in kernel ring buffer, visible via `dmesg`.

Patroni will log information about the watchdog when it's successfully enabled.