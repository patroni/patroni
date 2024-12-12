"""Implements *global_config* facilities.

The :class:`GlobalConfig` object is instantiated on import and replaces
``patroni.global_config`` module in :data:`sys.modules`, what allows to use
its properties and methods like they were module variables and functions.
"""
import sys
import types

from copy import deepcopy
from typing import Any, cast, Dict, List, Optional, TYPE_CHECKING

from .collections import EMPTY_DICT
from .utils import parse_bool, parse_int

if TYPE_CHECKING:  # pragma: no cover
    from .dcs import Cluster


def __getattr__(mod: types.ModuleType, name: str) -> Any:
    """This function exists just to make pyright happy.

    Without it pyright complains about access to unknown members of global_config module.
    """
    return getattr(sys.modules[__name__], name)  # pragma: no cover


class GlobalConfig(types.ModuleType):
    """A class that wraps global configuration and provides convenient methods to access/check values."""

    __file__ = __file__  # just to make unittest and pytest happy

    def __init__(self) -> None:
        """Initialize :class:`GlobalConfig` object."""
        super().__init__(__name__)
        self.__config = {}

    @staticmethod
    def _cluster_has_valid_config(cluster: Optional['Cluster']) -> bool:
        """Check if provided *cluster* object has a valid global configuration.

        :param cluster: the currently known cluster state from DCS.

        :returns: ``True`` if provided *cluster* object has a valid global configuration, otherwise ``False``.
        """
        return bool(cluster and cluster.config and cluster.config.modify_version)

    def update(self, cluster: Optional['Cluster'], default: Optional[Dict[str, Any]] = None) -> None:
        """Update with the new global configuration from the :class:`Cluster` object view.

        .. note::
            Update happens in-place and is executed only from the main heartbeat thread.

        :param cluster: the currently known cluster state from DCS.
        :param default: default configuration, which will be used if there is no valid *cluster.config*.
        """
        # Try to protect from the case when DCS was wiped out
        if self._cluster_has_valid_config(cluster):
            self.__config = cluster.config.data  # pyright: ignore [reportOptionalMemberAccess]
        elif default:
            self.__config = default

    def from_cluster(self, cluster: Optional['Cluster']) -> 'GlobalConfig':
        """Return :class:`GlobalConfig` instance from the provided :class:`Cluster` object view.

        .. note::
            If the provided *cluster* object doesn't have a valid global configuration we return
            the last known valid state of the :class:`GlobalConfig` object.

            This method is used when we need to have the most up-to-date values in the global configuration,
            but we don't want to update the global object.

        :param cluster: the currently known cluster state from DCS.

        :returns: :class:`GlobalConfig` object.
        """
        if not self._cluster_has_valid_config(cluster):
            return self

        ret = GlobalConfig()
        ret.update(cluster)
        return ret

    def get(self, name: str) -> Any:
        """Gets global configuration value by *name*.

        :param name: parameter name.

        :returns: configuration value or ``None`` if it is missing.
        """
        return self.__config.get(name)

    def check_mode(self, mode: str) -> bool:
        """Checks whether the certain parameter is enabled.

        :param mode: parameter name, e.g. ``synchronous_mode``, ``failsafe_mode``, ``pause``, ``check_timeline``, and
            so on.

        :returns: ``True`` if parameter *mode* is enabled in the global configuration.
        """
        return bool(parse_bool(self.__config.get(mode)))

    @property
    def is_paused(self) -> bool:
        """``True`` if cluster is in maintenance mode."""
        return self.check_mode('pause')

    @property
    def is_quorum_commit_mode(self) -> bool:
        """:returns: ``True`` if quorum commit replication is requested"""
        return str(self.get('synchronous_mode')).lower() == 'quorum'

    @property
    def is_synchronous_mode(self) -> bool:
        """``True`` if synchronous replication is requested and it is not a standby cluster config."""
        return (self.check_mode('synchronous_mode') is True or self.is_quorum_commit_mode) \
            and not self.is_standby_cluster

    @property
    def is_synchronous_mode_strict(self) -> bool:
        """``True`` if at least one synchronous node is required."""
        return self.check_mode('synchronous_mode_strict')

    def get_standby_cluster_config(self) -> Any:
        """Get ``standby_cluster`` configuration.

        :returns: a copy of ``standby_cluster`` configuration.
        """
        return deepcopy(self.get('standby_cluster'))

    @property
    def is_standby_cluster(self) -> bool:
        """``True`` if global configuration has a valid ``standby_cluster`` section."""
        config = self.get_standby_cluster_config()
        return isinstance(config, dict) and\
            any(cast(Dict[str, Any], config).get(p) for p in ('host', 'port', 'restore_command'))

    def get_int(self, name: str, default: int = 0, base_unit: Optional[str] = None) -> int:
        """Gets current value of *name* from the global configuration and try to return it as :class:`int`.

        :param name: name of the parameter.
        :param default: default value if *name* is not in the configuration or invalid.
        :param base_unit: an optional base unit to convert value of *name* parameter to.
                          Not used if the value does not contain a unit.

        :returns: currently configured value of *name* from the global configuration or *default* if it is not set or
            invalid.
        """
        ret = parse_int(self.get(name), base_unit)
        return default if ret is None else ret

    @property
    def min_synchronous_nodes(self) -> int:
        """The minimum number of synchronous nodes based on whether ``synchronous_mode_strict`` is enabled or not."""
        return 1 if self.is_synchronous_mode_strict else 0

    @property
    def synchronous_node_count(self) -> int:
        """Currently configured value of ``synchronous_node_count`` from the global configuration.

        Assume ``1`` if it is not set or invalid.
        """
        return max(self.get_int('synchronous_node_count', 1), self.min_synchronous_nodes)

    @property
    def maximum_lag_on_failover(self) -> int:
        """Currently configured value of ``maximum_lag_on_failover`` from the global configuration.

        Assume ``1048576`` if it is not set or invalid.
        """
        return self.get_int('maximum_lag_on_failover', 1048576)

    @property
    def maximum_lag_on_syncnode(self) -> int:
        """Currently configured value of ``maximum_lag_on_syncnode`` from the global configuration.

        Assume ``-1`` if it is not set or invalid.
        """
        return self.get_int('maximum_lag_on_syncnode', -1)

    @property
    def primary_start_timeout(self) -> int:
        """Currently configured value of ``primary_start_timeout`` from the global configuration.

        Assume ``300`` if it is not set or invalid.

        .. note::
            ``master_start_timeout`` is still supported to keep backward compatibility.
        """
        default = 300
        return self.get_int('primary_start_timeout', default)\
            if 'primary_start_timeout' in self.__config else self.get_int('master_start_timeout', default)

    @property
    def primary_stop_timeout(self) -> int:
        """Currently configured value of ``primary_stop_timeout`` from the global configuration.

        Assume ``0`` if it is not set or invalid.

        .. note::
            ``master_stop_timeout`` is still supported to keep backward compatibility.
        """
        default = 0
        return self.get_int('primary_stop_timeout', default)\
            if 'primary_stop_timeout' in self.__config else self.get_int('master_stop_timeout', default)

    @property
    def ignore_slots_matchers(self) -> List[Dict[str, Any]]:
        """Currently configured value of ``ignore_slots`` from the global configuration.

        Assume an empty :class:`list` if not set.
        """
        return self.get('ignore_slots') or []

    @property
    def max_timelines_history(self) -> int:
        """Currently configured value of ``max_timelines_history`` from the global configuration.

        Assume ``0`` if not set or invalid.
        """
        return self.get_int('max_timelines_history', 0)

    @property
    def use_slots(self) -> bool:
        """``True`` if cluster is configured to use replication slots."""
        return bool(parse_bool((self.get('postgresql') or EMPTY_DICT).get('use_slots', True)))

    @property
    def permanent_slots(self) -> Dict[str, Any]:
        """Dictionary of permanent slots information from the global configuration."""
        return deepcopy(self.get('permanent_replication_slots')
                        or self.get('permanent_slots')
                        or self.get('slots')
                        or EMPTY_DICT.copy())

    @property
    def member_slots_ttl(self) -> int:
        """Currently configured value of ``member_slots_ttl`` from the global configuration converted to seconds.

        Assume ``1800`` if it is not set or invalid.
        """
        return self.get_int('member_slots_ttl', 1800, base_unit='s')


sys.modules[__name__] = GlobalConfig()
