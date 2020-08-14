Security Considerations
=============================

A Patroni cluster has two interfaces to be protected from unauthorized access: the distributed configuration storage (DCS) and the Patroni REST API.

Protecting DCS
---------------

Patroni and patronictl both store and retrieve data to/from the DCS. 

Despite DCS doesn't contain any sensitive information, it allows changing some of Patroni/Postgres configuration. Therefore the very first thing that should be protected is DCS itself.

The details of protection depend on the type of DCS used. The authentication and encryption parameters (tokens/basic-auth/client certificates) for the supported types of DCS are covered in SETTINGS.

The general recommendation is to enable TLS for all DCS communication.

Protecting the REST API
-----------------------

The Patroni REST API used by Patroni itself during the leader race, by the ``patronictl`` tool in order to perform failovers/switchovers/reinitialize/restarts/reloads, by HAProxy or any other kind of load balancer to perform HTTP health checks, and of course could also be used for monitoring. 

From the point of view of security, REST API contains safe (``GET`` requests, only retrieve information) and unsafe (``PUT``, ``POST``, ``PATCH`` and ``DELETE`` requests, change the state of nodes) endpoints.

Unsafe endpoints can be protected with HTTP basic-auth by setting the restapi.authentication.username and restapi.authentication.password parameters. There is no way to protect the safe endpoints without enabling TLS.

When TLS for REST API is enabled and a PKI is established, depending on the value of the restapi.verify_client parameter, Patroni can be configured to require a successful client certificate verification for all API calls (verify_client: required), or only for unsafe API calls (verify_client: optional), or no API calls (verify_client: optional).

Protecting the PostgreSQL database proper from unauthorized access is beyond the scope of this document and is covered in https://www.postgresql.org/docs/current/client-authentication.html
