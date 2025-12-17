import logging
from typing import List, Dict, Any, Callable

from pysyncobj import replicated_sync

from .. import Postgresql
from ..connection import get_connection_cursor
from ...dcs import Member

logger = logging.getLogger(__name__)


class ReplicaUpgradePlugin:
    def after_primary_stop(self, postgresql: Postgresql, replicas: Dict[str, Member]):
        pass

    def before_pg_upgrade(self, postgresql: Postgresql, replicas: Dict[str, Member]):
        pass

    def before_primary_start(self, ha: 'Ha', leader: Member, replicas: Dict[str, Member]) -> List[str]:
        pass

    @classmethod
    def get_plugin(cls, name: str, kwargs: Dict) -> "ReplicaUpgradePlugin":
        #TODO: implement this properly
        if name == 'rsync':
            from .replicarsync import ReplicaRsync
            return ReplicaRsync(**kwargs)

        from .replicareinit import ReplicaReinit
        return ReplicaReinit(**kwargs)

class UpgradePreparePlugin:
    per_db = False

    def __init__(self, config: Dict[str, Any]):
        pass

    def prepare_database(self, postgresql: Postgresql, cur, dbname: str):
        pass

    def post_upgrade(self):
        pass

    def post_process_database(self, postgresql: Postgresql, cur, dbname: str):
        pass

class UpgradePluginManager:
    registry : Dict[str, Callable[[Dict[str, Any]], UpgradePreparePlugin]] = {}

    @classmethod
    def register(cls, plugin: Callable[[Dict[str, Any]], UpgradePreparePlugin]):
        name = plugin.name if hasattr(plugin, 'name') else plugin.__name__
        cls.registry[name] = plugin

    @classmethod
    def get_plugin(cls, name: str, args: Dict) -> UpgradePreparePlugin:
        import patroni.postgresql.upgrade_plugins.prepare

        return cls.registry[name](args)

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        self.prepare_plugins: List[UpgradePreparePlugin] = self._resolve_plugin_list('prepare')
        self.post_upgrade_plugins: List[UpgradePreparePlugin] = self._resolve_plugin_list('post_upgrade')

    def _resolve_plugin_list(self, section_name: str) -> list[UpgradePreparePlugin]:
        plugin_configs = self.config.get(section_name, [])
        if not isinstance(plugin_configs, list):
            raise TypeError(f"'{section_name}' must be a list of plugins")

        logger.info(f"{section_name} plugins: {plugin_configs}")
        configs_ = [self._resolve_plugin(plugin_config) for plugin_config in plugin_configs]
        return configs_

    def _resolve_plugin(self, plugin_config) -> UpgradePreparePlugin:
        if isinstance(plugin_config, dict):
            if len(plugin_config) != 1:
                raise ValueError("Each 'prepare' entry must be a single entry map or a string")
            name, config = list(plugin_config.items())[0]
        elif isinstance(plugin_config, str):
            name, config = plugin_config, {}
        else:
            raise ValueError("Each 'prepare' entry must be a single entry map or a string")

        plugin = self.get_plugin(name, config)
        return plugin

    def _run_on_all_databases(self, postgresql: Postgresql, actions: List[Callable]):
        conn_kwargs = postgresql.local_conn_kwargs

        for db in postgresql.get_all_databases():
            conn_kwargs['dbname'] = db
            with get_connection_cursor(**conn_kwargs) as cur:
                for func in actions:
                    func(postgresql, cur, db)

    def prepare(self, postgresql: Postgresql):
        """Prepare source version for upgrade.

        Executed after upgrade has been checked, but while postgres is still running.

        :param postgresql: Postgresql instance for source version.
        :return:
        """
        actions = [plugin.prepare_database for plugin in self.prepare_plugins if plugin.per_db]
        self._run_on_all_databases(postgresql, actions)

    def post_upgrade(self, postgresql: Postgresql):
        """Hook point for running post processing actions after upgrade.

        :return:
        """
        per_db_actions = []

        for plugin in self.post_upgrade_plugins:
            if plugin.per_db:
                per_db_actions.append(plugin.post_process_database)
            else:
                plugin.post_upgrade()

        if per_db_actions:
            self._run_on_all_databases(postgresql, per_db_actions)
