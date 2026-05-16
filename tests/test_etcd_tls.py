"""Unit tests for patroni.dcs.etcd_tls."""
import ssl
import pytest

from patroni.dcs.etcd_tls import apply_tls_config, _cn_fallback_supported


class FakePool:
    """Mimic the urllib3.PoolManager surface that apply_tls_config touches."""
    def __init__(self, initial=None):
        self.connection_pool_kw = dict(initial or {})


def test_defaults_are_noop():
    """No flags set -> no changes to pool kwargs."""
    pool = FakePool()
    apply_tls_config(pool, {})
    assert pool.connection_pool_kw == {}


def test_cacert_only_is_noop():
    """Existing cacert config -> no changes (preserves backward compat)."""
    pool = FakePool({"ca_certs": "/some/ca.pem"})
    apply_tls_config(pool, {"ca_cert": "/some/ca.pem"})
    assert pool.connection_pool_kw.get("ca_certs") == "/some/ca.pem"
    assert "ssl_context" not in pool.connection_pool_kw


def test_verify_false_disables_everything():
    """verify=false -> CA off, hostname off, ca_certs cleared."""
    pool = FakePool({"ca_certs": "/some/ca.pem"})
    apply_tls_config(pool, {"verify": False})
    ctx = pool.connection_pool_kw["ssl_context"]
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False
    assert pool.connection_pool_kw["cert_reqs"] == "CERT_NONE"
    assert pool.connection_pool_kw["assert_hostname"] is False
    assert "ca_certs" not in pool.connection_pool_kw


def test_verify_hostname_false_keeps_ca():
    """verify_hostname=false -> CA still validated, hostname check off."""
    pool = FakePool()
    apply_tls_config(pool, {"verify_hostname": False})
    ctx = pool.connection_pool_kw["ssl_context"]
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is False
    assert pool.connection_pool_kw["assert_hostname"] is False


@pytest.mark.skipif(not _cn_fallback_supported(),
                    reason="OpenSSL build does not allow setting hostname_checks_common_name")
def test_cn_fallback_when_supported():
    """hostname_checks_common_name=true -> CN fallback enabled, CA still checked."""
    pool = FakePool()
    apply_tls_config(pool, {"hostname_checks_common_name": True})
    ctx = pool.connection_pool_kw["ssl_context"]
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    assert ctx.hostname_checks_common_name is True


@pytest.mark.skipif(_cn_fallback_supported(),
                    reason="OpenSSL build does support setting hostname_checks_common_name")
def test_cn_fallback_raises_on_unsupported_build():
    """hostname_checks_common_name=true on FIPS build -> clear RuntimeError."""
    with pytest.raises(RuntimeError, match="verify_hostname=false instead"):
        apply_tls_config(FakePool(), {"hostname_checks_common_name": True})


def test_warning_logged_for_verify_false(caplog):
    """verify=false must log a WARNING."""
    import logging
    caplog.set_level(logging.WARNING)
    apply_tls_config(FakePool(), {"verify": False})
    assert any("verify=false" in r.message and "DISABLED" in r.message
               for r in caplog.records)


def test_warning_logged_for_verify_hostname_false(caplog):
    """verify_hostname=false must log a WARNING (CA still validated)."""
    import logging
    caplog.set_level(logging.WARNING)
    apply_tls_config(FakePool(), {"verify_hostname": False})
    assert any("verify_hostname=false" in r.message
               for r in caplog.records)
