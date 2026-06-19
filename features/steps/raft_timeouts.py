from behave import step

# Custom pysyncobj timeout values injected into the `raft` section. They are
# chosen to satisfy the ordering constraints enforced in
# patroni.dcs.raft.KVStoreTTL.__init__ (min_timeout > 3 * append_entries_period,
# max_timeout > min_timeout, connection_timeout >= max_timeout, and
# leader_fallback_timeout > append_entries_period) and to differ from
# pysyncobj's defaults so that the "Applying custom pysyncobj timeouts" log
# line is emitted at startup.
RAFT_TIMEOUT_CONFIG = {
    'raft': {
        'append_entries_period': 1.0,
        'min_timeout': 5.0,
        'max_timeout': 10.0,
        'connection_timeout': 15.0,
        'connection_retry_time': 8.0,
        'leader_fallback_timeout': 60.0,
    }
}


@step('I start {name:name} with custom raft timeouts')
def start_patroni_with_custom_raft_timeouts(context, name):
    return context.pctl.start(name, custom_config=RAFT_TIMEOUT_CONFIG)
