.. _tools_integration:

Integration with other tools
============================

Patroni is able to integrate with other tools in your stack. In this section you
will find a list of examples, which although not an exhaustive list, might
provide you with ideas on how Patroni can integrate with other tools.

Barman
------

Patroni delivers an application named ``patroni_barman`` which has logic to
communicate with ``pg-backup-api``, so you are able to perform Barman operations
remotely.

This application currently has a couple of sub-commands: ``recover`` and
``config-switch``.

patroni_barman recover
^^^^^^^^^^^^^^^^^^^^^^

The ``recover`` sub-command can be used as a custom bootstrap or custom replica
creation method. You can find more information about that in
:ref:`replica_imaging_and_bootstrap`.

patroni_barman config-switch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``config-switch`` sub-command is designed to be used as an ``on_role_change``
callback in Patroni. As an example, assume you are streaming WALs from your
current primary to your Barman host. In the event of a failover in the cluster
you might want to start streaming WALs from the new primary. You can accomplish
this by using ``patroni_barman config-switch`` as the ``on_role_change`` callback.

.. note::
    That sub-command relies on the ``barman config-switch`` command, which is in
    charge of overriding the configuration of a Barman server by applying a
    pre-defined model on top of it. This command is available since Barman 3.10.
    Please consult the Barman documentation for more details.

This is an example of how you can configure Patroni to apply a configuration
model in case this Patroni node is promoted to primary:

.. code:: YAML

    postgresql:
        callbacks:
            on_role_change: >
                patroni_barman
                    --api-url YOUR_API_URL
                    config-switch
                    --barman-server YOUR_BARMAN_SERVER_NAME
                    --barman-model YOUR_BARMAN_MODEL_NAME
                    --switch-when promoted

.. note::
    ``patroni_barman config-switch`` requires that you have both Barman and
    ``pg-backup-api`` configured in the Barman host, so it can execute a remote
    ``barman config-switch`` through the backup API. Also, it requires that you
    have pre-configured Barman models to be applied. The above example uses a
    subset of the available parameters. You can get more information running
    ``patroni_barman config-switch --help``, and by consulting the Barman
    documentation.
