.. _security:

=======================
Security Considerations
=======================

A Patroni cluster has two interfaces to be protected from unauthorized access: the distributed configuration storage (DCS) and the Patroni REST API.

Protecting DCS
==============

Patroni and :ref:`patronictl` both store and retrieve data to/from the DCS.

Despite DCS doesn't contain any sensitive information, it allows changing some of Patroni/Postgres configuration. Therefore the very first thing that should be protected is DCS itself.

The details of protection depend on the type of DCS used. The authentication and encryption parameters (tokens/basic-auth/client certificates) for the supported types of DCS are covered in :ref:`settings <yaml_configuration>`.

The general recommendation is to enable TLS for all DCS communication.

Protecting the REST API
=======================

Protecting the REST API is a more complicated task.

The Patroni REST API is used by Patroni itself during the leader race, by the :ref:`patronictl` tool in order to perform failovers/switchovers/reinitialize/restarts/reloads, by HAProxy or any other kind of load balancer to perform HTTP health checks, and of course could also be used for monitoring.

From the point of view of security, REST API contains safe (``GET`` requests, only retrieve information) and unsafe (``PUT``, ``POST``, ``PATCH`` and ``DELETE`` requests, change the state of nodes) endpoints.

The unsafe endpoints can be protected with HTTP basic-auth by setting the ``restapi.authentication.username`` and ``restapi.authentication.password`` parameters. There is no way to protect the safe endpoints without enabling TLS.

When TLS for the REST API is enabled and a PKI is established, mutual authentication of the API server and API client is possible for all endpoints.

The ``restapi`` section parameters enable TLS client authentication to the server. Depending on the value of the ``verify_client`` parameter, the API server requires a successful client certificate verification for both safe and unsafe API calls (``verify_client: required``), or only for unsafe API calls (``verify_client: optional``), or for no API calls (``verify_client: none``).

The ``ctl`` section parameters enable TLS server authentication to the client (the :ref:`patronictl` tool which uses the same config as patroni). Set ``insecure: true`` to disable the server certificate verification by the client. See :ref:`settings <patronictl_settings>` for a detailed description of the TLS client parameters.

Protecting the PostgreSQL database proper from unauthorized access is beyond the scope of this document and is covered in https://www.postgresql.org/docs/current/client-authentication.html
