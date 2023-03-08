import abc
import logging
import six

from collections import namedtuple
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


class Bool(namedtuple('Bool', 'version_from,version_till')):

    @staticmethod
    def transform(name, value):
        if parse_bool(value) is not None:
            return value
        logger.warning('Removing bool parameter=%s from the config due to the invalid value=%s', name, value)


@six.add_metaclass(abc.ABCMeta)
class Number(namedtuple('Number', 'version_from,version_till,min_val,max_val,unit')):

    @staticmethod
    @abc.abstractmethod
    def parse(value, unit):
        """parse value"""

    def transform(self, name, value):
        num_value = self.parse(value, self.unit)
        if num_value is not None:
            if num_value < self.min_val:
                logger.warning('Value=%s of parameter=%s is too low, increasing to %s%s',
                               value, name, self.min_val, self.unit or '')
                return self.min_val
            if num_value > self.max_val:
                logger.warning('Value=%s of parameter=%s is too big, decreasing to %s%s',
                               value, name, self.max_val, self.unit or '')
                return self.max_val
            return value
        logger.warning('Removing %s parameter=%s from the config due to the invalid value=%s',
                       self.__class__.__name__.lower(), name, value)


class Integer(Number):

    @staticmethod
    def parse(value, unit):
        return parse_int(value, unit)


class Real(Number):

    @staticmethod
    def parse(value, unit):
        return parse_real(value, unit)


class Enum(namedtuple('Enum', 'version_from,version_till,possible_values')):

    def transform(self, name, value):
        if str(value).lower() in self.possible_values:
            return value
        logger.warning('Removing enum parameter=%s from the config due to the invalid value=%s', name, value)


class EnumBool(Enum):

    def transform(self, name, value):
        if parse_bool(value) is not None:
            return value
        return super(EnumBool, self).transform(name, value)


class String(namedtuple('String', 'version_from,version_till')):

    @staticmethod
    def transform(name, value):
        return value


# Format:
#  key - parameter name
#  value - tuple or multiple tuples if something was changing in GUC across postgres versions
parameters = CaseInsensitiveDict({
    'allow_in_place_tablespaces': Bool(150000, None),
    'allow_system_table_mods': Bool(90300, None),
    'application_name': String(90300, None),
    'archive_command': String(90300, None),
    'archive_library': String(150000, None),
    'archive_mode': (
        Bool(90300, 90500),
        EnumBool(90500, None, ('always',))
    ),
    'archive_timeout': Integer(90300, None, 0, 1073741823, 's'),
    'array_nulls': Bool(90300, None),
    'authentication_timeout': Integer(90300, None, 1, 600, 's'),
    'autovacuum': Bool(90300, None),
    'autovacuum_analyze_scale_factor': Real(90300, None, 0, 100, None),
    'autovacuum_analyze_threshold': Integer(90300, None, 0, 2147483647, None),
    'autovacuum_freeze_max_age': Integer(90300, None, 100000, 2000000000, None),
    'autovacuum_max_workers': (
        Integer(90300, 90600, 1, 8388607, None),
        Integer(90600, None, 1, 262143, None)
    ),
    'autovacuum_multixact_freeze_max_age': Integer(90300, None, 10000, 2000000000, None),
    'autovacuum_naptime': Integer(90300, None, 1, 2147483, 's'),
    'autovacuum_vacuum_cost_delay': (
        Integer(90300, 120000, -1, 100, 'ms'),
        Real(120000, None, -1, 100, 'ms')
    ),
    'autovacuum_vacuum_cost_limit': Integer(90300, None, -1, 10000, None),
    'autovacuum_vacuum_insert_scale_factor': Real(130000, None, 0, 100, None),
    'autovacuum_vacuum_insert_threshold': Integer(130000, None, -1, 2147483647, None),
    'autovacuum_vacuum_scale_factor': Real(90300, None, 0, 100, None),
    'autovacuum_vacuum_threshold': Integer(90300, None, 0, 2147483647, None),
    'autovacuum_work_mem': Integer(90400, None, -1, 2147483647, 'kB'),
    'backend_flush_after': Integer(90600, None, 0, 256, '8kB'),
    'backslash_quote': EnumBool(90300, None, ('safe_encoding',)),
    'backtrace_functions': String(130000, None),
    'bgwriter_delay': Integer(90300, None, 10, 10000, 'ms'),
    'bgwriter_flush_after': Integer(90600, None, 0, 256, '8kB'),
    'bgwriter_lru_maxpages': (
        Integer(90300, 100000, 0, 1000, None),
        Integer(100000, None, 0, 1073741823, None)
    ),
    'bgwriter_lru_multiplier': Real(90300, None, 0, 10, None),
    'bonjour': Bool(90300, None),
    'bonjour_name': String(90300, None),
    'bytea_output': Enum(90300, None, ('escape', 'hex')),
    'check_function_bodies': Bool(90300, None),
    'checkpoint_completion_target': Real(90300, None, 0, 1, None),
    'checkpoint_flush_after': Integer(90600, None, 0, 256, '8kB'),
    'checkpoint_segments': Integer(90300, 90500, 1, 2147483647, None),
    'checkpoint_timeout': (
        Integer(90300, 90600, 30, 3600, 's'),
        Integer(90600, None, 30, 86400, 's')
    ),
    'checkpoint_warning': Integer(90300, None, 0, 2147483647, 's'),
    'client_connection_check_interval': Integer(140000, None, 0, 2147483647, 'ms'),
    'client_encoding': String(90300, None),
    'client_min_messages': Enum(90300, None, ('debug5', 'debug4', 'debug3', 'debug2',
                                              'debug1', 'log', 'notice', 'warning', 'error')),
    'cluster_name': String(90500, None),
    'commit_delay': Integer(90300, None, 0, 100000, None),
    'commit_siblings': Integer(90300, None, 0, 1000, None),
    'compute_query_id': (
        EnumBool(140000, 150000, ('auto',)),
        EnumBool(150000, None, ('auto', 'regress'))
    ),
    'config_file': String(90300, None),
    'constraint_exclusion': EnumBool(90300, None, ('partition',)),
    'cpu_index_tuple_cost': Real(90300, None, 0, 1.79769e+308, None),
    'cpu_operator_cost': Real(90300, None, 0, 1.79769e+308, None),
    'cpu_tuple_cost': Real(90300, None, 0, 1.79769e+308, None),
    'cursor_tuple_fraction': Real(90300, None, 0, 1, None),
    'data_directory': String(90300, None),
    'data_sync_retry': Bool(90400, None),
    'DateStyle': String(90300, None),
    'db_user_namespace': Bool(90300, None),
    'deadlock_timeout': Integer(90300, None, 1, 2147483647, 'ms'),
    'debug_discard_caches': Integer(150000, None, 0, 0, None),
    'debug_pretty_print': Bool(90300, None),
    'debug_print_parse': Bool(90300, None),
    'debug_print_plan': Bool(90300, None),
    'debug_print_rewritten': Bool(90300, None),
    'default_statistics_target': Integer(90300, None, 1, 10000, None),
    'default_table_access_method': String(120000, None),
    'default_tablespace': String(90300, None),
    'default_text_search_config': String(90300, None),
    'default_toast_compression': Enum(140000, None, ('pglz', 'lz4')),
    'default_transaction_deferrable': Bool(90300, None),
    'default_transaction_isolation': Enum(90300, None, ('serializable', 'repeatable read',
                                                        'read committed', 'read uncommitted')),
    'default_transaction_read_only': Bool(90300, None),
    'default_with_oids': Bool(90300, 120000),
    'dynamic_library_path': String(90300, None),
    'dynamic_shared_memory_type': (
        Enum(90400, 120000, ('posix', 'sysv', 'mmap', 'none')),
        Enum(120000, None, ('posix', 'sysv', 'mmap'))
    ),
    'effective_cache_size': Integer(90300, None, 1, 2147483647, '8kB'),
    'effective_io_concurrency': Integer(90300, None, 0, 1000, None),
    'enable_async_append': Bool(140000, None),
    'enable_bitmapscan': Bool(90300, None),
    'enable_gathermerge': Bool(100000, None),
    'enable_hashagg': Bool(90300, None),
    'enable_hashjoin': Bool(90300, None),
    'enable_incremental_sort': Bool(130000, None),
    'enable_indexonlyscan': Bool(90300, None),
    'enable_indexscan': Bool(90300, None),
    'enable_material': Bool(90300, None),
    'enable_memoize': Bool(150000, None),
    'enable_mergejoin': Bool(90300, None),
    'enable_nestloop': Bool(90300, None),
    'enable_parallel_append': Bool(110000, None),
    'enable_parallel_hash': Bool(110000, None),
    'enable_partition_pruning': Bool(110000, None),
    'enable_partitionwise_aggregate': Bool(110000, None),
    'enable_partitionwise_join': Bool(110000, None),
    'enable_seqscan': Bool(90300, None),
    'enable_sort': Bool(90300, None),
    'enable_tidscan': Bool(90300, None),
    'escape_string_warning': Bool(90300, None),
    'event_source': String(90300, None),
    'exit_on_error': Bool(90300, None),
    'extension_destdir': String(140000, None),
    'external_pid_file': String(90300, None),
    'extra_float_digits': Integer(90300, None, -15, 3, None),
    'force_parallel_mode': EnumBool(90600, None, ('regress',)),
    'from_collapse_limit': Integer(90300, None, 1, 2147483647, None),
    'fsync': Bool(90300, None),
    'full_page_writes': Bool(90300, None),
    'geqo': Bool(90300, None),
    'geqo_effort': Integer(90300, None, 1, 10, None),
    'geqo_generations': Integer(90300, None, 0, 2147483647, None),
    'geqo_pool_size': Integer(90300, None, 0, 2147483647, None),
    'geqo_seed': Real(90300, None, 0, 1, None),
    'geqo_selection_bias': Real(90300, None, 1.5, 2, None),
    'geqo_threshold': Integer(90300, None, 2, 2147483647, None),
    'gin_fuzzy_search_limit': Integer(90300, None, 0, 2147483647, None),
    'gin_pending_list_limit': Integer(90500, None, 64, 2147483647, 'kB'),
    'hash_mem_multiplier': Real(130000, None, 1, 1000, None),
    'hba_file': String(90300, None),
    'hot_standby': Bool(90300, None),
    'hot_standby_feedback': Bool(90300, None),
    'huge_pages': EnumBool(90400, None, ('try',)),
    'huge_page_size': Integer(140000, None, 0, 2147483647, 'kB'),
    'ident_file': String(90300, None),
    'idle_in_transaction_session_timeout': Integer(90600, None, 0, 2147483647, 'ms'),
    'idle_session_timeout': Integer(140000, None, 0, 2147483647, 'ms'),
    'ignore_checksum_failure': Bool(90300, None),
    'ignore_invalid_pages': Bool(130000, None),
    'ignore_system_indexes': Bool(90300, None),
    'IntervalStyle': Enum(90300, None, ('postgres', 'postgres_verbose', 'sql_standard', 'iso_8601')),
    'jit': Bool(110000, None),
    'jit_above_cost': Real(110000, None, -1, 1.79769e+308, None),
    'jit_debugging_support': Bool(110000, None),
    'jit_dump_bitcode': Bool(110000, None),
    'jit_expressions': Bool(110000, None),
    'jit_inline_above_cost': Real(110000, None, -1, 1.79769e+308, None),
    'jit_optimize_above_cost': Real(110000, None, -1, 1.79769e+308, None),
    'jit_profiling_support': Bool(110000, None),
    'jit_provider': String(110000, None),
    'jit_tuple_deforming': Bool(110000, None),
    'join_collapse_limit': Integer(90300, None, 1, 2147483647, None),
    'krb_caseins_users': Bool(90300, None),
    'krb_server_keyfile': String(90300, None),
    'krb_srvname': String(90300, 90400),
    'lc_messages': String(90300, None),
    'lc_monetary': String(90300, None),
    'lc_numeric': String(90300, None),
    'lc_time': String(90300, None),
    'listen_addresses': String(90300, None),
    'local_preload_libraries': String(90300, None),
    'lock_timeout': Integer(90300, None, 0, 2147483647, 'ms'),
    'lo_compat_privileges': Bool(90300, None),
    'log_autovacuum_min_duration': Integer(90300, None, -1, 2147483647, 'ms'),
    'log_checkpoints': Bool(90300, None),
    'log_connections': Bool(90300, None),
    'log_destination': String(90300, None),
    'log_directory': String(90300, None),
    'log_disconnections': Bool(90300, None),
    'log_duration': Bool(90300, None),
    'log_error_verbosity': Enum(90300, None, ('terse', 'default', 'verbose')),
    'log_executor_stats': Bool(90300, None),
    'log_file_mode': Integer(90300, None, 0, 511, None),
    'log_filename': String(90300, None),
    'logging_collector': Bool(90300, None),
    'log_hostname': Bool(90300, None),
    'logical_decoding_work_mem': Integer(130000, None, 64, 2147483647, 'kB'),
    'log_line_prefix': String(90300, None),
    'log_lock_waits': Bool(90300, None),
    'log_min_duration_sample': Integer(130000, None, -1, 2147483647, 'ms'),
    'log_min_duration_statement': Integer(90300, None, -1, 2147483647, 'ms'),
    'log_min_error_statement': Enum(90300, None, ('debug5', 'debug4', 'debug3', 'debug2', 'debug1', 'info',
                                                  'notice', 'warning', 'error', 'log', 'fatal', 'panic')),
    'log_min_messages': Enum(90300, None, ('debug5', 'debug4', 'debug3', 'debug2', 'debug1', 'info',
                                           'notice', 'warning', 'error', 'log', 'fatal', 'panic')),
    'log_parameter_max_length': Integer(130000, None, -1, 1073741823, 'B'),
    'log_parameter_max_length_on_error': Integer(130000, None, -1, 1073741823, 'B'),
    'log_parser_stats': Bool(90300, None),
    'log_planner_stats': Bool(90300, None),
    'log_recovery_conflict_waits': Bool(140000, None),
    'log_replication_commands': Bool(90500, None),
    'log_rotation_age': Integer(90300, None, 0, 35791394, 'min'),
    'log_rotation_size': Integer(90300, None, 0, 2097151, 'kB'),
    'log_startup_progress_interval': Integer(150000, None, 0, 2147483647, 'ms'),
    'log_statement': Enum(90300, None, ('none', 'ddl', 'mod', 'all')),
    'log_statement_sample_rate': Real(130000, None, 0, 1, None),
    'log_statement_stats': Bool(90300, None),
    'log_temp_files': Integer(90300, None, -1, 2147483647, 'kB'),
    'log_timezone': String(90300, None),
    'log_transaction_sample_rate': Real(120000, None, 0, 1, None),
    'log_truncate_on_rotation': Bool(90300, None),
    'maintenance_io_concurrency': Integer(130000, None, 0, 1000, None),
    'maintenance_work_mem': Integer(90300, None, 1024, 2147483647, 'kB'),
    'max_connections': (
        Integer(90300, 90600, 1, 8388607, None),
        Integer(90600, None, 1, 262143, None)
    ),
    'max_files_per_process': (
        Integer(90300, 130000, 25, 2147483647, None),
        Integer(130000, None, 64, 2147483647, None)
    ),
    'max_locks_per_transaction': Integer(90300, None, 10, 2147483647, None),
    'max_logical_replication_workers': Integer(100000, None, 0, 262143, None),
    'max_parallel_maintenance_workers': Integer(110000, None, 0, 1024, None),
    'max_parallel_workers': Integer(100000, None, 0, 1024, None),
    'max_parallel_workers_per_gather': Integer(90600, None, 0, 1024, None),
    'max_pred_locks_per_page': Integer(100000, None, 0, 2147483647, None),
    'max_pred_locks_per_relation': Integer(100000, None, -2147483648, 2147483647, None),
    'max_pred_locks_per_transaction': Integer(90300, None, 10, 2147483647, None),
    'max_prepared_transactions': (
        Integer(90300, 90600, 0, 8388607, None),
        Integer(90600, None, 0, 262143, None)
    ),
    'max_replication_slots': (
        Integer(90400, 90600, 0, 8388607, None),
        Integer(90600, None, 0, 262143, None)
    ),
    'max_slot_wal_keep_size': Integer(130000, None, -1, 2147483647, 'MB'),
    'max_stack_depth': Integer(90300, None, 100, 2147483647, 'kB'),
    'max_standby_archive_delay': Integer(90300, None, -1, 2147483647, 'ms'),
    'max_standby_streaming_delay': Integer(90300, None, -1, 2147483647, 'ms'),
    'max_sync_workers_per_subscription': Integer(100000, None, 0, 262143, None),
    'max_wal_senders': (
        Integer(90300, 90600, 0, 8388607, None),
        Integer(90600, None, 0, 262143, None)
    ),
    'max_wal_size': (
        Integer(90500, 100000, 2, 2147483647, '16MB'),
        Integer(100000, None, 2, 2147483647, 'MB')
    ),
    'max_worker_processes': (
        Integer(90400, 90600, 1, 8388607, None),
        Integer(90600, None, 0, 262143, None)
    ),
    'min_dynamic_shared_memory': Integer(140000, None, 0, 2147483647, 'MB'),
    'min_parallel_index_scan_size': Integer(100000, None, 0, 715827882, '8kB'),
    'min_parallel_relation_size': Integer(90600, 100000, 0, 715827882, '8kB'),
    'min_parallel_table_scan_size': Integer(100000, None, 0, 715827882, '8kB'),
    'min_wal_size': (
        Integer(90500, 100000, 2, 2147483647, '16MB'),
        Integer(100000, None, 2, 2147483647, 'MB')
    ),
    'old_snapshot_threshold': Integer(90600, None, -1, 86400, 'min'),
    'operator_precedence_warning': Bool(90500, 140000),
    'parallel_leader_participation': Bool(110000, None),
    'parallel_setup_cost': Real(90600, None, 0, 1.79769e+308, None),
    'parallel_tuple_cost': Real(90600, None, 0, 1.79769e+308, None),
    'password_encryption': (
        Bool(90300, 100000),
        Enum(100000, None, ('md5', 'scram-sha-256'))
    ),
    'plan_cache_mode': Enum(120000, None, ('auto', 'force_generic_plan', 'force_custom_plan')),
    'port': Integer(90300, None, 1, 65535, None),
    'post_auth_delay': Integer(90300, None, 0, 2147, 's'),
    'pre_auth_delay': Integer(90300, None, 0, 60, 's'),
    'quote_all_identifiers': Bool(90300, None),
    'random_page_cost': Real(90300, None, 0, 1.79769e+308, None),
    'recovery_init_sync_method': Enum(140000, None, ('fsync', 'syncfs')),
    'recovery_prefetch': EnumBool(150000, None, ('try',)),
    'recursive_worktable_factor': Real(150000, None, 0.001, 1e+06, None),
    'remove_temp_files_after_crash': Bool(140000, None),
    'replacement_sort_tuples': Integer(90600, 110000, 0, 2147483647, None),
    'restart_after_crash': Bool(90300, None),
    'row_security': Bool(90500, None),
    'search_path': String(90300, None),
    'seq_page_cost': Real(90300, None, 0, 1.79769e+308, None),
    'session_preload_libraries': String(90400, None),
    'session_replication_role': Enum(90300, None, ('origin', 'replica', 'local')),
    'shared_buffers': Integer(90300, None, 16, 1073741823, '8kB'),
    'shared_memory_type': Enum(120000, None, ('sysv', 'mmap')),
    'shared_preload_libraries': String(90300, None),
    'sql_inheritance': Bool(90300, 100000),
    'ssl': Bool(90300, None),
    'ssl_ca_file': String(90300, None),
    'ssl_cert_file': String(90300, None),
    'ssl_ciphers': String(90300, None),
    'ssl_crl_dir': String(140000, None),
    'ssl_crl_file': String(90300, None),
    'ssl_dh_params_file': String(100000, None),
    'ssl_ecdh_curve': String(90400, None),
    'ssl_key_file': String(90300, None),
    'ssl_max_protocol_version': Enum(120000, None, ('', 'tlsv1', 'tlsv1.1', 'tlsv1.2', 'tlsv1.3')),
    'ssl_min_protocol_version': Enum(120000, None, ('tlsv1', 'tlsv1.1', 'tlsv1.2', 'tlsv1.3')),
    'ssl_passphrase_command': String(110000, None),
    'ssl_passphrase_command_supports_reload': Bool(110000, None),
    'ssl_prefer_server_ciphers': Bool(90400, None),
    'ssl_renegotiation_limit': Integer(90300, 90500, 0, 2147483647, 'kB'),
    'standard_conforming_strings': Bool(90300, None),
    'statement_timeout': Integer(90300, None, 0, 2147483647, 'ms'),
    'stats_fetch_consistency': Enum(150000, None, ('none', 'cache', 'snapshot')),
    'stats_temp_directory': String(90300, 150000),
    'superuser_reserved_connections': (
        Integer(90300, 90600, 0, 8388607, None),
        Integer(90600, None, 0, 262143, None)
    ),
    'synchronize_seqscans': Bool(90300, None),
    'synchronous_commit': (
        EnumBool(90300, 90600, ('local', 'remote_write')),
        EnumBool(90600, None, ('local', 'remote_write', 'remote_apply'))
    ),
    'synchronous_standby_names': String(90300, None),
    'syslog_facility': Enum(90300, None, ('local0', 'local1', 'local2', 'local3',
                                          'local4', 'local5', 'local6', 'local7')),
    'syslog_ident': String(90300, None),
    'syslog_sequence_numbers': Bool(90600, None),
    'syslog_split_messages': Bool(90600, None),
    'tcp_keepalives_count': Integer(90300, None, 0, 2147483647, None),
    'tcp_keepalives_idle': Integer(90300, None, 0, 2147483647, 's'),
    'tcp_keepalives_interval': Integer(90300, None, 0, 2147483647, 's'),
    'tcp_user_timeout': Integer(120000, None, 0, 2147483647, 'ms'),
    'temp_buffers': Integer(90300, None, 100, 1073741823, '8kB'),
    'temp_file_limit': Integer(90300, None, -1, 2147483647, 'kB'),
    'temp_tablespaces': String(90300, None),
    'TimeZone': String(90300, None),
    'timezone_abbreviations': String(90300, None),
    'trace_notify': Bool(90300, None),
    'trace_recovery_messages': Enum(90300, None, ('debug5', 'debug4', 'debug3', 'debug2',
                                                  'debug1', 'log', 'notice', 'warning', 'error')),
    'trace_sort': Bool(90300, None),
    'track_activities': Bool(90300, None),
    'track_activity_query_size': (
        Integer(90300, 110000, 100, 102400, None),
        Integer(110000, 130000, 100, 102400, 'B'),
        Integer(130000, None, 100, 1048576, 'B')
    ),
    'track_commit_timestamp': Bool(90500, None),
    'track_counts': Bool(90300, None),
    'track_functions': Enum(90300, None, ('none', 'pl', 'all')),
    'track_io_timing': Bool(90300, None),
    'track_wal_io_timing': Bool(140000, None),
    'transaction_deferrable': Bool(90300, None),
    'transaction_isolation': Enum(90300, None, ('serializable', 'repeatable read',
                                                'read committed', 'read uncommitted')),
    'transaction_read_only': Bool(90300, None),
    'transform_null_equals': Bool(90300, None),
    'unix_socket_directories': String(90300, None),
    'unix_socket_group': String(90300, None),
    'unix_socket_permissions': Integer(90300, None, 0, 511, None),
    'update_process_title': Bool(90300, None),
    'vacuum_cleanup_index_scale_factor': Real(110000, 140000, 0, 1e+10, None),
    'vacuum_cost_delay': (
        Integer(90300, 120000, 0, 100, 'ms'),
        Real(120000, None, 0, 100, 'ms')
    ),
    'vacuum_cost_limit': Integer(90300, None, 1, 10000, None),
    'vacuum_cost_page_dirty': Integer(90300, None, 0, 10000, None),
    'vacuum_cost_page_hit': Integer(90300, None, 0, 10000, None),
    'vacuum_cost_page_miss': Integer(90300, None, 0, 10000, None),
    'vacuum_defer_cleanup_age': Integer(90300, None, 0, 1000000, None),
    'vacuum_failsafe_age': Integer(140000, None, 0, 2100000000, None),
    'vacuum_freeze_min_age': Integer(90300, None, 0, 1000000000, None),
    'vacuum_freeze_table_age': Integer(90300, None, 0, 2000000000, None),
    'vacuum_multixact_failsafe_age': Integer(140000, None, 0, 2100000000, None),
    'vacuum_multixact_freeze_min_age': Integer(90300, None, 0, 1000000000, None),
    'vacuum_multixact_freeze_table_age': Integer(90300, None, 0, 2000000000, None),
    'wal_buffers': Integer(90300, None, -1, 262143, '8kB'),
    'wal_compression': (
        Bool(90500, 150000),
        EnumBool(150000, None, ('pglz', 'lz4', 'zstd'))
    ),
    'wal_consistency_checking': String(100000, None),
    'wal_decode_buffer_size': Integer(150000, None, 65536, 1073741823, 'B'),
    'wal_init_zero': Bool(120000, None),
    'wal_keep_segments': Integer(90300, 130000, 0, 2147483647, None),
    'wal_keep_size': Integer(130000, None, 0, 2147483647, 'MB'),
    'wal_level': (
        Enum(90300, 90400, ('minimal', 'archive', 'hot_standby')),
        Enum(90400, 90600, ('minimal', 'archive', 'hot_standby', 'logical')),
        Enum(90600, None, ('minimal', 'replica', 'logical'))
    ),
    'wal_log_hints': Bool(90400, None),
    'wal_receiver_create_temp_slot': Bool(130000, None),
    'wal_receiver_status_interval': Integer(90300, None, 0, 2147483, 's'),
    'wal_receiver_timeout': Integer(90300, None, 0, 2147483647, 'ms'),
    'wal_recycle': Bool(120000, None),
    'wal_retrieve_retry_interval': Integer(90500, None, 1, 2147483647, 'ms'),
    'wal_sender_timeout': Integer(90300, None, 0, 2147483647, 'ms'),
    'wal_skip_threshold': Integer(130000, None, 0, 2147483647, 'kB'),
    'wal_sync_method': Enum(90300, None, ('fsync', 'fdatasync', 'open_sync', 'open_datasync')),
    'wal_writer_delay': Integer(90300, None, 1, 10000, 'ms'),
    'wal_writer_flush_after': Integer(90600, None, 0, 2147483647, '8kB'),
    'work_mem': Integer(90300, None, 64, 2147483647, 'kB'),
    'xmlbinary': Enum(90300, None, ('base64', 'hex')),
    'xmloption': Enum(90300, None, ('content', 'document')),
    'zero_damaged_pages': Bool(90300, None)
})


recovery_parameters = CaseInsensitiveDict({
    'archive_cleanup_command': String(90300, None),
    'pause_at_recovery_target': Bool(90300, 90500),
    'primary_conninfo': String(90300, None),
    'primary_slot_name': String(90400, None),
    'promote_trigger_file': String(120000, None),
    'recovery_end_command': String(90300, None),
    'recovery_min_apply_delay': Integer(90400, None, 0, 2147483647, 'ms'),
    'recovery_target': Enum(90400, None, ('immediate', '')),
    'recovery_target_action': Enum(90500, None, ('pause', 'promote', 'shutdown')),
    'recovery_target_inclusive': Bool(90300, None),
    'recovery_target_lsn': String(100000, None),
    'recovery_target_name': String(90400, None),
    'recovery_target_time': String(90300, None),
    'recovery_target_timeline': String(90300, None),
    'recovery_target_xid': String(90300, None),
    'restore_command': String(90300, None),
    'standby_mode': Bool(90300, 120000),
    'trigger_file': String(90300, 120000)
})


def _transform_parameter_value(validators, version, name, value):
    validators = validators.get(name)
    if validators:
        for validator in (validators if isinstance(validators[0], tuple) else [validators]):
            if version >= validator.version_from and\
                    (validator.version_till is None or version < validator.version_till):
                return validator.transform(name, value)
    logger.warning('Removing unexpected parameter=%s value=%s from the config', name, value)


def transform_postgresql_parameter_value(version, name, value):
    if '.' in name:
        return value
    if name in recovery_parameters:
        return None
    return _transform_parameter_value(parameters, version, name, value)


def transform_recovery_parameter_value(version, name, value):
    return _transform_parameter_value(recovery_parameters, version, name, value)
