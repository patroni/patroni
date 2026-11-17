.. _watchdog:

Watchdog support
================

Having multiple PostgreSQL servers running as primary can result in transactions lost due to diverging timelines. This situation is also called a split-brain problem. To avoid split-brain Patroni needs to ensure PostgreSQL will not accept any transaction commits after leader key expires in the DCS. Under normal circumstances Patroni will try to achieve this by stopping PostgreSQL when leader lock update fails for any reason. However, this may fail to happen due to various reasons:

- Patroni has crashed due to a bug, out-of-memory condition or by being accidentally killed by a system administrator.

- Shutting down PostgreSQL is too slow.

- Patroni does not get to run due to high load on the system, the VM being paused by the hypervisor, or other infrastructure issues.

To guarantee correct behavior under these conditions Patroni supports watchdog devices. Watchdog devices are software or hardware mechanisms that will reset the whole system when they do not get a keepalive heartbeat within a specified timeframe. This adds an additional layer of fail safe in case usual Patroni split-brain protection mechanisms fail.

Patroni will try to activate the watchdog before promoting PostgreSQL to primary. If watchdog activation fails and watchdog mode is ``required`` then the node will refuse to become leader. When deciding to participate in leader election Patroni will also check that watchdog configuration will allow it to become leader at all. After demoting PostgreSQL (for example due to a manual failover) Patroni will disable the watchdog again. Watchdog will also be disabled while Patroni is in paused state.

By default Patroni will set up the watchdog to expire 5 seconds before TTL expires. With the default setup of ``loop_wait=10`` and ``ttl=30`` this gives HA loop at least 15 seconds (``ttl`` - ``safety_margin`` - ``loop_wait``) to complete before the system gets forcefully reset. By default accessing DCS is configured to time out after 10 seconds. This means that when DCS is unavailable, for example due to network issues, Patroni and PostgreSQL will have at least 5 seconds (``ttl`` - ``safety_margin`` - ``loop_wait`` - ``retry_timeout``) to come to a state where all client connections are terminated.

Safety margin is the amount of time that Patroni reserves for time between leader key update and watchdog keepalive. Patroni will try to send a keepalive immediately after confirmation of leader key update. If Patroni process is suspended for extended amount of time at exactly the right moment the keepalive may be delayed for more than the safety margin without triggering the watchdog. This results in a window of time where watchdog will not trigger before leader key expiration, invalidating the guarantee. To be absolutely sure that watchdog will trigger under all circumstances set up the watchdog to expire after half of TTL by setting ``safety_margin`` to -1 to set watchdog timeout to ``ttl // 2``. If you need this guarantee you probably should increase ``ttl`` and/or reduce ``loop_wait`` and ``retry_timeout``.

During demotion, once PostgreSQL has started shutting down, Patroni can disable the watchdog as soon as all user backends have exited. If there are no user backends, it can disable the watchdog without waiting for the remaining PostgreSQL shutdown work to finish.

Patroni supports watchdog devices through the Linux watchdog device interface. In ``automatic`` mode,
``watchdog.fallback`` defaults to ``none``, so no thread fallback is used. Setting it to ``thread`` enables
the fallback described below. ``required`` mode requires a usable watchdog device and never uses the thread
fallback; ``off`` disables both implementations.

With the fallback enabled, Patroni can use a thread if a device is unavailable, cannot provide a suitable timeout,
or fails to accept a keepalive. The thread uses the same configured timeout and safety margin as a device,
and its timeout must be at least ``loop_wait``. Patroni logs a warning when activating the thread fallback.
Timeout changes and enabling the fallback trigger a device timeout suitability check on the next keepalive.

Patroni replaces an active device with the thread fallback only after confirming that the device has stopped.
On Linux, this requires a successful explicit disable ioctl; closing the device alone does not confirm this.
If disabling fails or is unsupported, Patroni does not switch to the thread fallback. A device that has disappeared,
for example because its driver was unbound, reports ``ENODEV`` and can no longer be pinged or stopped, so Patroni
releases it and switches to the thread fallback.

Thread watchdog fallback
------------------------

To enable the fallback explicitly:

.. code-block:: yaml

        watchdog:
            mode: automatic
            fallback: thread
            safety_margin: 5

The watchdog thread is started during Patroni initialization and waits while disarmed. Activation, disabling, and
configuration changes reuse this same thread rather than starting or stopping a worker.
Reloading ``fallback: none`` disarms an active fallback immediately. Enabling the fallback while the watchdog is
active takes effect on the next keepalive. Changing only this setting does not replace a working device watchdog.

The watchdog thread tracks HA-loop keepalives using a monotonic clock. If no keepalive arrives before the timeout,
it logs a critical message, requests immediate PostgreSQL shutdown, and forcibly exits Patroni without running normal
shutdown handlers.
This bypasses checkpoints and user-defined shutdown callbacks. It does not wait for PostgreSQL shutdown to finish.
If requesting the shutdown takes longer than 5 seconds, Patroni writes the stacks of all its threads to standard
error and exits anyway. On POSIX systems, if writing the stacks blocks, for example because the reader of standard
error is stalled, ``SIGALRM`` terminates Patroni after 10 seconds.
As with a device watchdog, the thread is disarmed when Patroni disables the watchdog, including during pause and
after demotion reaches a safe point. Unlike a device, the thread cannot outlive Patroni, so if Patroni is shutting
down but could not stop PostgreSQL, it keeps running until the armed thread expires. The stop timeout of the service
manager, for example ``TimeoutStopSec`` of systemd, should therefore exceed the watchdog timeout.

This fallback is best-effort protection against a stalled HA loop, not a substitute for a watchdog device.
It cannot run if Patroni is killed or suspended, if Python cannot schedule the watchdog thread, or if the operating
system or virtual machine is stalled. It cannot guarantee that PostgreSQL will process the shutdown request before
the leader key expires, and it does not reboot the host. Use ``required`` mode with a watchdog device when those
stronger guarantees are needed.

Setting up software watchdog on Linux
-------------------------------------

Default Patroni configuration will try to use ``/dev/watchdog`` on Linux if it is accessible to Patroni. For most use cases using software watchdog built into the Linux kernel is secure enough.

To enable software watchdog issue the following commands as root before starting Patroni:

.. code-block:: bash

    modprobe softdog
    # Replace postgres with the user you will be running patroni under
    chown postgres /dev/watchdog

For testing it may be helpful to disable rebooting by adding ``soft_noboot=1`` to the modprobe command line. In this case the watchdog will just log a line in kernel ring buffer, visible via `dmesg`.

Patroni will log information about the watchdog when it is successfully enabled.
