def is_standby_cluster(config):
    """ Check whether or not provided configuration describes a standby cluster.
        Config can be both patroni config or cluster.config.data
    """
    return isinstance(config, dict) and (
        config.get('host') is not None or
        config.get('port') is not None or
        config.get('restore_command') is not None
    )
