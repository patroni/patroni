import logging

from urllib3.response import HTTPHeaderDict

from ..utils import parse_bool, parse_int, parse_real

logger = logging.getLogger(__name__)


class CaseInsensitiveDict(HTTPHeaderDict):

    def add(self, key, val):
        self[key] = val

    def __getitem__(self, key):
        return self._container[key.lower()][1]

    def __repr__(self):
        return str(dict(self.items()))

    def copy(self):
        return CaseInsensitiveDict(self._container.values())


# Format:
#  key - parameter name
#  value - tuple or multiple tuples if something was changing in GUC across postgres versions
#  tuple - (version_from, version_till, vartype, ...)
#    version_from - the minimal version of postgres this parameter exists (supported in patroni)
#    version_till - the version of postgres where the parameter was removed or changed
#    vartype - possible values: bool, integer, real, enum, string
#
#    for integer and real there are additional values:
#      min_val - the minimal possible value accepted by postgres
#      max_val - the maximum possible value
#      unit - ms, s, B, kB, 8kB, MB, 16MB
#    for enum there is additional value - the tuple with possible enum values
parameters = CaseInsensitiveDict({
    'allow_system_table_mods': (90300, None, 'bool'),
    'application_name': (90300, None, 'string'),
    'archive_command': (90300, None, 'string'),
    'archive_mode': (
        (90300, 90500, 'bool'),
        (90500, None, 'enum', ('always', 'on', 'off'))
    ),
    'archive_timeout': (90300, None, 'integer', 0, 1073741823, 's'),
    'array_nulls': (90300, None, 'bool'),
    'authentication_timeout': (90300, None, 'integer', 1, 600, 's'),
    'autovacuum': (90300, None, 'bool'),
    'autovacuum_analyze_scale_factor': (90300, None, 'real', 0, 100, None),
    'autovacuum_analyze_threshold': (90300, None, 'integer', 0, 2147483647, None),
    'autovacuum_freeze_max_age': (90300, None, 'integer', 100000, 2000000000, None),
    'autovacuum_max_workers': (
        (90300, 90600, 'integer', 1, 8388607, None),
        (90600, None, 'integer', 1, 262143, None)
    ),
    'autovacuum_multixact_freeze_max_age': (90300, None, 'integer', 10000, 2000000000, None),
    'autovacuum_naptime': (90300, None, 'integer', 1, 2147483, 's'),
    'autovacuum_vacuum_cost_delay': (
        (90300, 120000, 'integer', -1, 100, 'ms'),
        (120000, None, 'real', -1, 100, 'ms')
    ),
    'autovacuum_vacuum_cost_limit': (90300, None, 'integer', -1, 10000, None),
    'autovacuum_vacuum_insert_scale_factor': (130000, None, 'real', 0, 100, None),
    'autovacuum_vacuum_insert_threshold': (130000, None, 'integer', -1, 2147483647, None),
    'autovacuum_vacuum_scale_factor': (90300, None, 'real', 0, 100, None),
    'autovacuum_vacuum_threshold': (90300, None, 'integer', 0, 2147483647, None),
    'autovacuum_work_mem': (90400, None, 'integer', -1, 2147483647, 'kB'),
    'backend_flush_after': (90600, None, 'integer', 0, 256, '8kB'),
    'backslash_quote': (90300, None, 'enum', ('safe_encoding', 'on', 'off')),
    'backtrace_functions': (130000, None, 'string'),
    'bgwriter_delay': (90300, None, 'integer', 10, 10000, 'ms'),
    'bgwriter_flush_after': (90600, None, 'integer', 0, 256, '8kB'),
    'bgwriter_lru_maxpages': (
        (90300, 100000, 'integer', 0, 1000, None),
        (100000, None, 'integer', 0, 1073741823, None)
    ),
    'bgwriter_lru_multiplier': (90300, None, 'real', 0, 10, None),
    'bonjour': (90300, None, 'bool'),
    'bonjour_name': (90300, None, 'string'),
    'bytea_output': (90300, None, 'enum', ('escape', 'hex')),
    'check_function_bodies': (90300, None, 'bool'),
    'checkpoint_completion_target': (90300, None, 'real', 0, 1, None),
    'checkpoint_flush_after': (90600, None, 'integer', 0, 256, '8kB'),
    'checkpoint_segments': (90300, 90500, 'integer', 1, 2147483647, None),
    'checkpoint_timeout': (
        (90300, 90600, 'integer', 30, 3600, 's'),
        (90600, None, 'integer', 30, 86400, 's')
    ),
    'checkpoint_warning': (90300, None, 'integer', 0, 2147483647, 's'),
    'client_encoding': (90300, None, 'string'),
    'client_min_messages': (90300, None, 'enum', ('debug5', 'debug4', 'debug3', 'debug2',
                                                  'debug1', 'log', 'notice', 'warning', 'error')),
    'cluster_name': (90500, None, 'string'),
    'commit_delay': (90300, None, 'integer', 0, 100000, None),
    'commit_siblings': (90300, None, 'integer', 0, 1000, None),
    'config_file': (90300, None, 'string'),
    'constraint_exclusion': (90300, None, 'enum', ('partition', 'on', 'off')),
    'cpu_index_tuple_cost': (90300, None, 'real', 0, 1.79769e+308, None),
    'cpu_operator_cost': (90300, None, 'real', 0, 1.79769e+308, None),
    'cpu_tuple_cost': (90300, None, 'real', 0, 1.79769e+308, None),
    'cursor_tuple_fraction': (90300, None, 'real', 0, 1, None),
    'data_directory': (90300, None, 'string'),
    'data_sync_retry': (90400, None, 'bool'),
    'DateStyle': (90300, None, 'string'),
    'db_user_namespace': (90300, None, 'bool'),
    'deadlock_timeout': (90300, None, 'integer', 1, 2147483647, 'ms'),
    'debug_pretty_print': (90300, None, 'bool'),
    'debug_print_parse': (90300, None, 'bool'),
    'debug_print_plan': (90300, None, 'bool'),
    'debug_print_rewritten': (90300, None, 'bool'),
    'default_statistics_target': (90300, None, 'integer', 1, 10000, None),
    'default_table_access_method': (120000, None, 'string'),
    'default_tablespace': (90300, None, 'string'),
    'default_text_search_config': (90300, None, 'string'),
    'default_transaction_deferrable': (90300, None, 'bool'),
    'default_transaction_isolation': (90300, None, 'enum', ('serializable', 'repeatable read',
                                                            'read committed', 'read uncommitted')),
    'default_transaction_read_only': (90300, None, 'bool'),
    'default_with_oids': (90300, 120000, 'bool'),
    'dynamic_library_path': (90300, None, 'string'),
    'dynamic_shared_memory_type': (
        (90400, 120000, 'enum', ('posix', 'sysv', 'mmap', 'none')),
        (120000, None, 'enum', ('posix', 'sysv', 'mmap'))
    ),
    'effective_cache_size': (90300, None, 'integer', 1, 2147483647, '8kB'),
    'effective_io_concurrency': (90300, None, 'integer', 0, 1000, None),
    'enable_bitmapscan': (90300, None, 'bool'),
    'enable_gathermerge': (100000, None, 'bool'),
    'enable_hashagg': (90300, None, 'bool'),
    'enable_hashjoin': (90300, None, 'bool'),
    'enable_incremental_sort': (130000, None, 'bool'),
    'enable_indexonlyscan': (90300, None, 'bool'),
    'enable_indexscan': (90300, None, 'bool'),
    'enable_material': (90300, None, 'bool'),
    'enable_mergejoin': (90300, None, 'bool'),
    'enable_nestloop': (90300, None, 'bool'),
    'enable_parallel_append': (110000, None, 'bool'),
    'enable_parallel_hash': (110000, None, 'bool'),
    'enable_partition_pruning': (110000, None, 'bool'),
    'enable_partitionwise_aggregate': (110000, None, 'bool'),
    'enable_partitionwise_join': (110000, None, 'bool'),
    'enable_seqscan': (90300, None, 'bool'),
    'enable_sort': (90300, None, 'bool'),
    'enable_tidscan': (90300, None, 'bool'),
    'escape_string_warning': (90300, None, 'bool'),
    'event_source': (90300, None, 'string'),
    'exit_on_error': (90300, None, 'bool'),
    'external_pid_file': (90300, None, 'string'),
    'extra_float_digits': (90300, None, 'integer', -15, 3, None),
    'force_parallel_mode': (90600, None, 'enum', ('off', 'on', 'regress')),
    'from_collapse_limit': (90300, None, 'integer', 1, 2147483647, None),
    'fsync': (90300, None, 'bool'),
    'full_page_writes': (90300, None, 'bool'),
    'geqo': (90300, None, 'bool'),
    'geqo_effort': (90300, None, 'integer', 1, 10, None),
    'geqo_generations': (90300, None, 'integer', 0, 2147483647, None),
    'geqo_pool_size': (90300, None, 'integer', 0, 2147483647, None),
    'geqo_seed': (90300, None, 'real', 0, 1, None),
    'geqo_selection_bias': (90300, None, 'real', 1.5, 2, None),
    'geqo_threshold': (90300, None, 'integer', 2, 2147483647, None),
    'gin_fuzzy_search_limit': (90300, None, 'integer', 0, 2147483647, None),
    'gin_pending_list_limit': (90500, None, 'integer', 64, 2147483647, 'kB'),
    'hash_mem_multiplier': (130000, None, 'real', 1, 1000, None),
    'hba_file': (90300, None, 'string'),
    'hot_standby': (90300, None, 'bool'),
    'hot_standby_feedback': (90300, None, 'bool'),
    'huge_pages': (90400, None, 'enum', ('off', 'on', 'try')),
    'ident_file': (90300, None, 'string'),
    'idle_in_transaction_session_timeout': (90600, None, 'integer', 0, 2147483647, 'ms'),
    'ignore_checksum_failure': (90300, None, 'bool'),
    'ignore_invalid_pages': (130000, None, 'bool'),
    'ignore_system_indexes': (90300, None, 'bool'),
    'IntervalStyle': (90300, None, 'enum', ('postgres', 'postgres_verbose', 'sql_standard', 'iso_8601')),
    'jit': (110000, None, 'bool'),
    'jit_above_cost': (110000, None, 'real', -1, 1.79769e+308, None),
    'jit_debugging_support': (110000, None, 'bool'),
    'jit_dump_bitcode': (110000, None, 'bool'),
    'jit_expressions': (110000, None, 'bool'),
    'jit_inline_above_cost': (110000, None, 'real', -1, 1.79769e+308, None),
    'jit_optimize_above_cost': (110000, None, 'real', -1, 1.79769e+308, None),
    'jit_profiling_support': (110000, None, 'bool'),
    'jit_provider': (110000, None, 'string'),
    'jit_tuple_deforming': (110000, None, 'bool'),
    'join_collapse_limit': (90300, None, 'integer', 1, 2147483647, None),
    'krb_caseins_users': (90300, None, 'bool'),
    'krb_server_keyfile': (90300, None, 'string'),
    'krb_srvname': (90300, 90400, 'string'),
    'lc_messages': (90300, None, 'string'),
    'lc_monetary': (90300, None, 'string'),
    'lc_numeric': (90300, None, 'string'),
    'lc_time': (90300, None, 'string'),
    'listen_addresses': (90300, None, 'string'),
    'local_preload_libraries': (90300, None, 'string'),
    'lock_timeout': (90300, None, 'integer', 0, 2147483647, 'ms'),
    'lo_compat_privileges': (90300, None, 'bool'),
    'log_autovacuum_min_duration': (90300, None, 'integer', -1, 2147483647, 'ms'),
    'log_checkpoints': (90300, None, 'bool'),
    'log_connections': (90300, None, 'bool'),
    'log_destination': (90300, None, 'string'),
    'log_directory': (90300, None, 'string'),
    'log_disconnections': (90300, None, 'bool'),
    'log_duration': (90300, None, 'bool'),
    'log_error_verbosity': (90300, None, 'enum', ('terse', 'default', 'verbose')),
    'log_executor_stats': (90300, None, 'bool'),
    'log_file_mode': (90300, None, 'integer', 0, 511, None),
    'log_filename': (90300, None, 'string'),
    'logging_collector': (90300, None, 'bool'),
    'log_hostname': (90300, None, 'bool'),
    'logical_decoding_work_mem': (130000, None, 'integer', 64, 2147483647, 'kB'),
    'log_line_prefix': (90300, None, 'string'),
    'log_lock_waits': (90300, None, 'bool'),
    'log_min_duration_sample': (130000, None, 'integer', -1, 2147483647, 'ms'),
    'log_min_duration_statement': (90300, None, 'integer', -1, 2147483647, 'ms'),
    'log_min_error_statement': (90300, None, 'enum', ('debug5', 'debug4', 'debug3', 'debug2', 'debug1', 'info',
                                                      'notice', 'warning', 'error', 'log', 'fatal', 'panic')),
    'log_min_messages': (90300, None, 'enum', ('debug5', 'debug4', 'debug3', 'debug2', 'debug1', 'info',
                                               'notice', 'warning', 'error', 'log', 'fatal', 'panic')),
    'log_parameter_max_length': (130000, None, 'integer', -1, 1073741823, 'B'),
    'log_parameter_max_length_on_error': (130000, None, 'integer', -1, 1073741823, 'B'),
    'log_parser_stats': (90300, None, 'bool'),
    'log_planner_stats': (90300, None, 'bool'),
    'log_replication_commands': (90500, None, 'bool'),
    'log_rotation_age': (90300, None, 'integer', 0, 35791394, 'min'),
    'log_rotation_size': (90300, None, 'integer', 0, 2097151, 'kB'),
    'log_statement': (90300, None, 'enum', ('none', 'ddl', 'mod', 'all')),
    'log_statement_sample_rate': (130000, None, 'real', 0, 1, None),
    'log_statement_stats': (90300, None, 'bool'),
    'log_temp_files': (90300, None, 'integer', -1, 2147483647, 'kB'),
    'log_timezone': (90300, None, 'string'),
    'log_transaction_sample_rate': (120000, None, 'real', 0, 1, None),
    'log_truncate_on_rotation': (90300, None, 'bool'),
    'maintenance_io_concurrency': (130000, None, 'integer', 0, 1000, None),
    'maintenance_work_mem': (90300, None, 'integer', 1024, 2147483647, 'kB'),
    'max_connections': (
        (90300, 90600, 'integer', 1, 8388607, None),
        (90600, None, 'integer', 1, 262143, None)
    ),
    'max_files_per_process': (
        (90300, 130000, 'integer', 25, 2147483647, None),
        (130000, None, 'integer', 64, 2147483647, None)
    ),
    'max_locks_per_transaction': (90300, None, 'integer', 10, 2147483647, None),
    'max_logical_replication_workers': (100000, None, 'integer', 0, 262143, None),
    'max_parallel_maintenance_workers': (110000, None, 'integer', 0, 1024, None),
    'max_parallel_workers': (100000, None, 'integer', 0, 1024, None),
    'max_parallel_workers_per_gather': (90600, None, 'integer', 0, 1024, None),
    'max_pred_locks_per_page': (100000, None, 'integer', 0, 2147483647, None),
    'max_pred_locks_per_relation': (100000, None, 'integer', -2147483648, 2147483647, None),
    'max_pred_locks_per_transaction': (90300, None, 'integer', 10, 2147483647, None),
    'max_prepared_transactions': (
        (90300, 90600, 'integer', 0, 8388607, None),
        (90600, None, 'integer', 0, 262143, None)
    ),
    'max_replication_slots': (
        (90400, 90600, 'integer', 0, 8388607, None),
        (90600, None, 'integer', 0, 262143, None)
    ),
    'max_slot_wal_keep_size': (130000, None, 'integer', -1, 2147483647, 'MB'),
    'max_stack_depth': (90300, None, 'integer', 100, 2147483647, 'kB'),
    'max_standby_archive_delay': (90300, None, 'integer', -1, 2147483647, 'ms'),
    'max_standby_streaming_delay': (90300, None, 'integer', -1, 2147483647, 'ms'),
    'max_sync_workers_per_subscription': (100000, None, 'integer', 0, 262143, None),
    'max_wal_senders': (
        (90300, 90600, 'integer', 0, 8388607, None),
        (90600, None, 'integer', 0, 262143, None)
    ),
    'max_wal_size': (
        (90500, 100000, 'integer', 2, 2147483647, '16MB'),
        (100000, None, 'integer', 2, 2147483647, 'MB')
    ),
    'max_worker_processes': (
        (90400, 90600, 'integer', 1, 8388607, None),
        (90600, None, 'integer', 0, 262143, None)
    ),
    'min_parallel_index_scan_size': (100000, None, 'integer', 0, 715827882, '8kB'),
    'min_parallel_relation_size': (90600, 100000, 'integer', 0, 715827882, '8kB'),
    'min_parallel_table_scan_size': (100000, None, 'integer', 0, 715827882, '8kB'),
    'min_wal_size': (
        (90500, 100000, 'integer', 2, 2147483647, '16MB'),
        (100000, None, 'integer', 2, 2147483647, 'MB')
    ),
    'old_snapshot_threshold': (90600, None, 'integer', -1, 86400, 'min'),
    'operator_precedence_warning': (90500, None, 'bool'),
    'parallel_leader_participation': (110000, None, 'bool'),
    'parallel_setup_cost': (90600, None, 'real', 0, 1.79769e+308, None),
    'parallel_tuple_cost': (90600, None, 'real', 0, 1.79769e+308, None),
    'password_encryption': (
        (90300, 100000, 'bool'),
        (100000, None, 'enum', ('md5', 'scram-sha-256'))
    ),
    'plan_cache_mode': (120000, None, 'enum', ('auto', 'force_generic_plan', 'force_custom_plan')),
    'port': (90300, None, 'integer', 1, 65535, None),
    'post_auth_delay': (90300, None, 'integer', 0, 2147, 's'),
    'pre_auth_delay': (90300, None, 'integer', 0, 60, 's'),
    'quote_all_identifiers': (90300, None, 'bool'),
    'random_page_cost': (90300, None, 'real', 0, 1.79769e+308, None),
    'replacement_sort_tuples': (90600, 110000, 'integer', 0, 2147483647, None),
    'restart_after_crash': (90300, None, 'bool'),
    'row_security': (90500, None, 'bool'),
    'search_path': (90300, None, 'string'),
    'seq_page_cost': (90300, None, 'real', 0, 1.79769e+308, None),
    'session_preload_libraries': (90400, None, 'string'),
    'session_replication_role': (90300, None, 'enum', ('origin', 'replica', 'local')),
    'shared_buffers': (90300, None, 'integer', 16, 1073741823, '8kB'),
    'shared_memory_type': (120000, None, 'enum', ('sysv', 'mmap')),
    'shared_preload_libraries': (90300, None, 'string'),
    'sql_inheritance': (90300, 100000, 'bool'),
    'ssl': (90300, None, 'bool'),
    'ssl_ca_file': (90300, None, 'string'),
    'ssl_cert_file': (90300, None, 'string'),
    'ssl_ciphers': (90300, None, 'string'),
    'ssl_crl_file': (90300, None, 'string'),
    'ssl_dh_params_file': (100000, None, 'string'),
    'ssl_ecdh_curve': (90400, None, 'string'),
    'ssl_key_file': (90300, None, 'string'),
    'ssl_max_protocol_version': (120000, None, 'enum', ('', 'tlsv1', 'tlsv1.1', 'tlsv1.2', 'tlsv1.3')),
    'ssl_min_protocol_version': (120000, None, 'enum', ('tlsv1', 'tlsv1.1', 'tlsv1.2', 'tlsv1.3')),
    'ssl_passphrase_command': (110000, None, 'string'),
    'ssl_passphrase_command_supports_reload': (110000, None, 'bool'),
    'ssl_prefer_server_ciphers': (90400, None, 'bool'),
    'ssl_renegotiation_limit': (90300, 90500, 'integer', 0, 2147483647, 'kB'),
    'standard_conforming_strings': (90300, None, 'bool'),
    'statement_timeout': (90300, None, 'integer', 0, 2147483647, 'ms'),
    'stats_temp_directory': (90300, None, 'string'),
    'superuser_reserved_connections': (
        (90300, 90600, 'integer', 0, 8388607, None),
        (90600, None, 'integer', 0, 262143, None),
    ),
    'synchronize_seqscans': (90300, None, 'bool'),
    'synchronous_commit': (
        (90300, 90600, 'enum', ('local', 'remote_write', 'on', 'off')),
        (90600, None, 'enum', ('local', 'remote_write', 'remote_apply', 'on', 'off'))
    ),
    'synchronous_standby_names': (90300, None, 'string'),
    'syslog_facility': (90300, None, 'enum', ('local0', 'local1', 'local2', 'local3',
                                              'local4', 'local5', 'local6', 'local7')),
    'syslog_ident': (90300, None, 'string'),
    'syslog_sequence_numbers': (90600, None, 'bool'),
    'syslog_split_messages': (90600, None, 'bool'),
    'tcp_keepalives_count': (90300, None, 'integer', 0, 2147483647, None),
    'tcp_keepalives_idle': (90300, None, 'integer', 0, 2147483647, 's'),
    'tcp_keepalives_interval': (90300, None, 'integer', 0, 2147483647, 's'),
    'tcp_user_timeout': (120000, None, 'integer', 0, 2147483647, 'ms'),
    'temp_buffers': (90300, None, 'integer', 100, 1073741823, '8kB'),
    'temp_file_limit': (90300, None, 'integer', -1, 2147483647, 'kB'),
    'temp_tablespaces': (90300, None, 'string'),
    'TimeZone': (90300, None, 'string'),
    'timezone_abbreviations': (90300, None, 'string'),
    'trace_notify': (90300, None, 'bool'),
    'trace_recovery_messages': (90300, None, 'enum', ('debug5', 'debug4', 'debug3', 'debug2',
                                                      'debug1', 'log', 'notice', 'warning', 'error')),
    'trace_sort': (90300, None, 'bool'),
    'track_activities': (90300, None, 'bool'),
    'track_activity_query_size': (
        (90300, 110000, 'integer', 100, 102400, None),
        (110000, 130000, 'integer', 100, 102400, 'B'),
        (130000, None, 'integer', 100, 1048576, 'B')
    ),
    'track_commit_timestamp': (90500, None, 'bool'),
    'track_counts': (90300, None, 'bool'),
    'track_functions': (90300, None, 'enum', ('none', 'pl', 'all')),
    'track_io_timing': (90300, None, 'bool'),
    'transaction_deferrable': (90300, None, 'bool'),
    'transaction_isolation': (90300, None, 'enum', ('serializable', 'repeatable read',
                                                    'read committed', 'read uncommitted')),
    'transaction_read_only': (90300, None, 'bool'),
    'transform_null_equals': (90300, None, 'bool'),
    'unix_socket_directories': (90300, None, 'string'),
    'unix_socket_group': (90300, None, 'string'),
    'unix_socket_permissions': (90300, None, 'integer', 0, 511, None),
    'update_process_title': (90300, None, 'bool'),
    'vacuum_cleanup_index_scale_factor': (110000, None, 'real', 0, 1e+10, None),
    'vacuum_cost_delay': (
        (90300, 120000, 'integer', 0, 100, 'ms'),
        (120000, None, 'real', 0, 100, 'ms')
    ),
    'vacuum_cost_limit': (90300, None, 'integer', 1, 10000, None),
    'vacuum_cost_page_dirty': (90300, None, 'integer', 0, 10000, None),
    'vacuum_cost_page_hit': (90300, None, 'integer', 0, 10000, None),
    'vacuum_cost_page_miss': (90300, None, 'integer', 0, 10000, None),
    'vacuum_defer_cleanup_age': (90300, None, 'integer', 0, 1000000, None),
    'vacuum_freeze_min_age': (90300, None, 'integer', 0, 1000000000, None),
    'vacuum_freeze_table_age': (90300, None, 'integer', 0, 2000000000, None),
    'vacuum_multixact_freeze_min_age': (90300, None, 'integer', 0, 1000000000, None),
    'vacuum_multixact_freeze_table_age': (90300, None, 'integer', 0, 2000000000, None),
    'wal_buffers': (90300, None, 'integer', -1, 262143, '8kB'),
    'wal_compression': (90500, None, 'bool'),
    'wal_consistency_checking': (100000, None, 'string'),
    'wal_init_zero': (120000, None, 'bool'),
    'wal_keep_segments': (90300, 130000, 'integer', 0, 2147483647, None),
    'wal_keep_size': (130000, None, 'integer', 0, 2147483647, 'MB'),
    'wal_level': (
        (90300, 90400, 'enum', ('minimal', 'archive', 'hot_standby')),
        (90400, 90600, 'enum', ('minimal', 'archive', 'hot_standby', 'logical')),
        (90600, None, 'enum', ('minimal', 'replica', 'logical'))
    ),
    'wal_log_hints': (90400, None, 'bool'),
    'wal_receiver_create_temp_slot': (130000, None, 'bool'),
    'wal_receiver_status_interval': (90300, None, 'integer', 0, 2147483, 's'),
    'wal_receiver_timeout': (90300, None, 'integer', 0, 2147483647, 'ms'),
    'wal_recycle': (120000, None, 'bool'),
    'wal_retrieve_retry_interval': (90500, None, 'integer', 1, 2147483647, 'ms'),
    'wal_sender_timeout': (90300, None, 'integer', 0, 2147483647, 'ms'),
    'wal_skip_threshold': (130000, None, 'integer', 0, 2147483647, 'kB'),
    'wal_sync_method': (90300, None, 'enum', ('fsync', 'fdatasync', 'open_sync', 'open_datasync')),
    'wal_writer_delay': (90300, None, 'integer', 1, 10000, 'ms'),
    'wal_writer_flush_after': (90600, None, 'integer', 0, 2147483647, '8kB'),
    'work_mem': (90300, None, 'integer', 64, 2147483647, 'kB'),
    'xmlbinary': (90300, None, 'enum', ('base64', 'hex')),
    'xmloption': (90300, None, 'enum', ('content', 'document')),
    'zero_damaged_pages': (90300, None, 'bool')
})


recovery_parameters = CaseInsensitiveDict({
    'archive_cleanup_command': (90300, None, 'string'),
    'pause_at_recovery_target': (90300, 90500, 'bool'),
    'primary_conninfo': (90300, None, 'string'),
    'primary_slot_name': (90400, None, 'string'),
    'promote_trigger_file': (120000, None, 'string'),
    'recovery_end_command': (90300, None, 'string'),
    'recovery_min_apply_delay': (90400, None, 'integer', 0, 2147483647, 'ms'),
    'recovery_target': (90400, None, 'enum', ('immediate',)),
    'recovery_target_action': (90500, None, 'enum', ('pause', 'promote', 'shutdown')),
    'recovery_target_inclusive': (90300, None, 'bool'),
    'recovery_target_lsn': (100000, None, 'string'),
    'recovery_target_name': (90400, None, 'string'),
    'recovery_target_time': (90300, None, 'string'),
    'recovery_target_timeline': (90300, None, 'string'),
    'recovery_target_xid': (90300, None, 'string'),
    'restore_command': (90300, None, 'string'),
    'standby_mode': (90300, 120000, 'bool'),
    'trigger_file': (90300, 120000, 'string')
})


def transform_bool(name, value):
    bool_value = parse_bool(value)
    if bool_value is not None:
        return value
    logger.warning('Removing bool parameter=%s from the config due to the invalid value=%s', name, value)


def _transform_number(name, value, validator):
    func = parse_int if validator[2] == 'int' else parse_real
    num_value = func(value, validator[5])
    if num_value is not None:
        if num_value < validator[3]:
            logger.warning('Value=%s of parameter=%s is too low, increasing to %s%s',
                           value, name, validator[3], validator[5] or '')
            return validator[3]
        if num_value > validator[4]:
            logger.warning('Value=%s of parameter=%s is too big, decreasing to %s%s',
                           value, name, validator[4], validator[5] or '')
            return validator[4]
        return value
    logger.warning('Removing %s parameter=%s from the config due to the invalid value=%s', validator[2], name, value)


def transform_enum(name, value, validator):
    if str(value).lower() in validator[3]:
        return value
    logger.warning('Removing enum parameter=%s from the config due to the invalid value=%s', name, value)


def _transform_parameter_value(validators, version, name, value):
    validators = validators.get(name)
    if validators:
        for validator in (validators if isinstance(validators[0], tuple) else [validators]):
            if version >= validator[0] and (validator[1] is None or version < validator[1]):
                converters = {
                    'bool': lambda v1, v2, v3: transform_bool(v1, v2),
                    'integer': _transform_number,
                    'real': _transform_number,
                    'enum': transform_enum,
                    'string': lambda v1, v2, v3: v2
                }
                return (converters.get(validator[2]) or converters['string'])(name, value, validator)
    logger.warning('Removing unexpected parameter=%s value=%s from the config', name, value)


def transform_postgresql_parameter_value(version, name, value):
    if '.' in name:
        return value
    return _transform_parameter_value(parameters, version, name, value)


def transform_recovery_parameter_value(version, name, value):
    return _transform_parameter_value(recovery_parameters, version, name, value)
