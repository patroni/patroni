

"""Abstract class for Formation Cluster Handler."""
import abc
from threading import Thread
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from patroni.dcs import Cluster

if TYPE_CHECKING:  # pragma: no cover
    from . import Postgresql


class AbstractHandler(Thread):

    def __init__(self, postgresql: 'Postgresql', config: Optional[Dict[str, Union[str, int]]]) -> None:
        super(AbstractHandler, self).__init__()
        self.daemon = True
        self._postgresql = postgresql
        self._config = config

    def is_enabled(self) -> bool:
        return isinstance(self._config, dict)

    @abc.abstractmethod
    def is_coordinator(self) -> bool:
        """Am I the Coordinator cluster of the formation?"""

    def is_worker(self) -> bool:
        """Am I a Worker cluster of the formation?"""
        return self.is_enabled() and not self.is_coordinator()

    def group(self) -> Optional[int]:
        return int(self._config['group']) if isinstance(self._config, dict) else None

    @abc.abstractmethod
    def bootstrap(self) -> None:
        """Behaviour should be taken after the PostgreSQL instance started."""

    @abc.abstractmethod
    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        """Handle events from restapi."""

    @abc.abstractmethod
    def schedule_cache_rebuild(self) -> None:
        """"""

    @abc.abstractmethod
    def sync_coordinator_meta(self, cluster: Cluster) -> None:
        """"""

    @abc.abstractmethod
    def on_demote(self) -> None:
        """Behaviour should be taken when demoting the instance."""

    @abc.abstractmethod
    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """"""

    @abc.abstractmethod
    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        """"""
