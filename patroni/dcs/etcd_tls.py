"""
TLS helpers for Patroni's etcd / etcd3 DCS backends.

Adds three optional, opt-in configuration flags that compose with the
existing `cacert` / `cert` / `key` parameters:

    verify: bool                       # default True  (CA chain validation)
    verify_hostname: bool              # default True  (SAN/CN identity check)
    hostname_checks_common_name: bool  # default False (allow legacy CN-only certs)

All flags default to the secure value, so existing configs behave exactly
as before. When a flag is relaxed, a single WARNING is logged at startup.
No global urllib3 state is touched.
"""

import logging
import ssl
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _cn_fallback_supported() -> bool:
    """
    Probe whether this Python/OpenSSL build allows setting
    SSLContext.hostname_checks_common_name = True.
    """
    try:
        probe = ssl.create_default_context()
        original = probe.hostname_checks_common_name
        probe.hostname_checks_common_name = True
        probe.hostname_checks_common_name = original
        return True
    except (AttributeError, Exception):
        return False


def apply_tls_config(http_pool: Any, config: Dict[str, Any]) -> None:
    """
    Apply the new opt-in TLS flags to an existing urllib3 PoolManager.

    Patroni's v2 etcd client inherits from python-etcd's etcd.Client, which
    builds self.http internally. We modify that pool in place rather than
    creating a new one.
    """
    verify = bool(config.get('verify', True))
    verify_hostname = bool(config.get('verify_hostname', True))
    cn_fallback = bool(config.get('hostname_checks_common_name', False))

    # Fast path: all defaults -> nothing to do.
    if verify and verify_hostname and not cn_fallback:
        return

    pool_kw = http_pool.connection_pool_kw

    # ---- Path 1: verification fully disabled (last resort, opt-in) ----
    if not verify:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        pool_kw['ssl_context'] = ctx
        pool_kw['cert_reqs'] = 'CERT_NONE'
        pool_kw['assert_hostname'] = False
        pool_kw.pop('ca_certs', None)
        logger.warning(
            "etcd TLS certificate verification is DISABLED by configuration "
            "(verify=false). This is insecure: any certificate from any "
            "issuer will be accepted. Use only as a temporary measure while "
            "migrating to a proper PKI."
        )
        return

    # ---- Path 2: verification enabled, with optional narrowing ----
    cafile = pool_kw.get('ca_certs') or config.get('ca_cert') or config.get('cacert')
    ctx = ssl.create_default_context(cafile=cafile)

    if cn_fallback:
        if not _cn_fallback_supported():
            raise RuntimeError(
                "hostname_checks_common_name=true was requested but this "
                "Python/OpenSSL build does not support setting it (commonly "
                "the case on FIPS-validated OpenSSL builds). Use "
                "verify_hostname=false instead for the same practical effect "
                "(CA chain still validated, only the SAN/CN identity check "
                "is skipped)."
            )
        try:
            ctx.hostname_checks_common_name = True
            logger.warning(
                "etcd TLS: hostname_checks_common_name=true. Certificates "
                "without subjectAltName will be matched against Common Name. "
                "CA chain validation remains enabled."
            )
        except AttributeError:
            raise RuntimeError(
                "hostname_checks_common_name=true cannot be applied on this "
                "build. Use verify_hostname=false instead."
            )

    if not verify_hostname:
        ctx.check_hostname = False
        pool_kw['assert_hostname'] = False
        logger.warning(
            "etcd TLS hostname verification is disabled "
            "(verify_hostname=false). CA chain validation remains enabled."
        )

    cert = pool_kw.get('cert_file')
    key = pool_kw.get('key_file')
    if cert:
        try:
            ctx.load_cert_chain(certfile=cert, keyfile=key)
        except Exception as e:
            logger.error("Failed to load client cert/key for etcd: %r", e)
            raise

    pool_kw['ssl_context'] = ctx
    pool_kw['cert_reqs'] = 'CERT_REQUIRED'
