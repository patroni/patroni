"""Abstract classes for MPP handler.

MPP stands for Massively Parallel Processing, and Citus belongs to this architecture. Currently, Citus is the only
supported MPP cluster. However, we may consider adapting other databases such as TimescaleDB, GPDB, etc. into Patroni.
"""
import abc

from typing import Any, Dict, Iterator, Optional, Tuple, Type, TYPE_CHECKING, Union

from ...dcs import Cluster
from ...dynamic_loader import iter_classes
from ...exceptions import PatroniException

if TYPE_CHECKING:  # pragma: no cover
    from ...config import Config
    from .. import Postgresql


class AbstractMPP(abc.ABC):
    """An abstract class which should be passed to :class:`AbstractDCS`.

    .. note::
        We create :class:`AbstractMPP` and :class:`AbstractMPPHandler` to solve the chicken-egg initialization problem.
        When initializing DCS, we dynamically create an object implementing :class:`AbstractMPP`, later this object is
        used to instantiate an object implementing :class:`AbstractMPPHandler`.
    """

    group_re: Any  # re.Pattern[str]

    def __init__(self, config: Dict[str, Union[str, int]]) -> None:
        """Init method for :class:`AbstractMPP`.

        :param config: configuration of MPP section.
        """
        self._config = config

    def is_enabled(self) -> bool:
        """Check if MPP is enabled for a given MPP.

        .. note::
            We just check that the :attr:`_config` object isn't empty and expect
            it to be empty only in case of :class:`Null`.

        :returns: ``True`` if MPP is enabled, otherwise ``False``.
        """
        return bool(self._config)

    @staticmethod
    @abc.abstractmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for a given MPP.

        :param config: configuration of MPP section.

        :returns: ``True`` is config passes validation, otherwise ``False``.
        """

    @property
    @abc.abstractmethod
    def group(self) -> Any:
        """The group for a given MPP implementation."""

    @property
    @abc.abstractmethod
    def coordinator_group_id(self) -> Any:
        """The group id of the coordinator PostgreSQL cluster."""

    @property
    def type(self) -> str:
        """The type of the MPP cluster.

        :returns: A string representation of the type of a given MPP implementation.
        """
        for base in self.__class__.__bases__:
            if not base.__name__.startswith('Abstract'):
                return base.__name__
        return self.__class__.__name__

    @property
    def k8s_group_label(self):
        """Group label used for kubernetes DCS of the MPP cluster.

        :returns: A string representation of the k8s group label of a given MPP implementation.
        """
        return self.type.lower() + '-group'

    def is_coordinator(self) -> bool:
        """Check whether this node is running in the coordinator PostgreSQL cluster.

        :returns: ``True`` if MPP is enabled and the group id of this node
                  matches with the :attr:`coordinator_group_id`, otherwise ``False``.
        """
        return self.is_enabled() and self.group == self.coordinator_group_id

    def is_worker(self) -> bool:
        """Check whether this node is running as a MPP worker PostgreSQL cluster.

        :returns: ``True`` if MPP is enabled and this node is known to be not running
                  as the coordinator PostgreSQL cluster, otherwise ``False``.
        """
        return self.is_enabled() and not self.is_coordinator()

    def _get_handler_cls(self) -> Iterator[Type['AbstractMPPHandler']]:
        """Find Handler classes inherited from a class type of this object.

        :yields: handler classes for this object.
        """
        for cls in self.__class__.__subclasses__():
            if issubclass(cls, AbstractMPPHandler) and cls.__name__.startswith(self.__class__.__name__):
                yield cls

    def get_handler_impl(self, postgresql: 'Postgresql') -> 'AbstractMPPHandler':
        """Find and instantiate Handler implementation of this object.

        :param postgresql: a reference to :class:`Postgresql` object.

        :raises:
            :exc:`PatroniException`: if the Handler class haven't been found.

        :returns: an instantiated class that implements Handler for this object.
        """
        for cls in self._get_handler_cls():
            return cls(postgresql, self._config)
        raise PatroniException(f'Failed to initialize {self.__class__.__name__}Handler object')


class AbstractMPPHandler(AbstractMPP):
    """An abstract class which defines interfaces that should be implemented by real handlers."""

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        """Init method for :class:`AbstractMPPHandler`.

        :param postgresql: a reference to :class:`Postgresql` object.
        :param config: configuration of MPP section.
        """
        super().__init__(config)
        self._postgresql = postgresql

    @abc.abstractmethod
    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        """Handle an event sent from a worker node.

        :param cluster: the currently known cluster state from DCS.
        :param event: the event to be handled.
        """

    @abc.abstractmethod
    def sync_meta_data(self, cluster: Cluster) -> None:
        """Sync meta data on the coordinator.

        :param cluster: the currently known cluster state from DCS.
        """

    @abc.abstractmethod
    def on_demote(self) -> None:
        """On demote handler.

        Is called when the primary was demoted.
        """

    @abc.abstractmethod
    def schedule_cache_rebuild(self) -> None:
        """Cache rebuild handler.

        Is called to notify handler that it has to refresh its metadata cache from the database.
        """

    @abc.abstractmethod
    def bootstrap(self) -> None:
        """Bootstrap handler.

        Is called when the new cluster is initialized (through ``initdb`` or a custom bootstrap method).
        """

    @abc.abstractmethod
    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """Adjust GUCs in the current PostgreSQL configuration.

        :param parameters: dictionary of GUCs, with key as GUC name and the corresponding value as current GUC value.
        """

    @abc.abstractmethod
    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        """Check whether provided replication *slot* existing in the database should not be removed.

        .. note::
            MPP database may create replication slots for its own use, for example to migrate data between workers
            using logical replication, and we don't want to suddenly drop them.

        :param slot: dictionary containing the replication slot settings, like ``name``, ``database``, ``type``, and
                     ``plugin``.

        :returns: ``True`` if the replication slots should not be removed, otherwise ``False``.
        """


class Null(AbstractMPP):
    """Dummy implementation of :class:`AbstractMPP`."""

    def __init__(self) -> None:
        """Init method for :class:`Null`."""
        super().__init__({})

    @staticmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for :class:`Null`.

        :returns: always ``True``.
        """
        return True

    @property
    def group(self) -> None:
        """The group for :class:`Null`.

        :returns: always ``None``.
        """
        return None

    @property
    def coordinator_group_id(self) -> None:
        """The group id of the coordinator PostgreSQL cluster.

        :returns: always ``None``.
        """
        return None


class NullHandler(Null, AbstractMPPHandler):
    """Dummy implementation of :class:`AbstractMPPHandler`."""

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        """Init method for :class:`NullHandler`.

        :param postgresql: a reference to :class:`Postgresql` object.
        :param config: configuration of MPP section.
        """
        AbstractMPPHandler.__init__(self, postgresql, config)

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        """Handle an event sent from a worker node.

        :param cluster: the currently known cluster state from DCS.
        :param event: the event to be handled.
        """

    def sync_meta_data(self, cluster: Cluster) -> None:
        """Sync meta data on the coordinator.

        :param cluster: the currently known cluster state from DCS.
        """

    def on_demote(self) -> None:
        """On demote handler.

        Is called when the primary was demoted.
        """

    def schedule_cache_rebuild(self) -> None:
        """Cache rebuild handler.

        Is called to notify handler that it has to refresh its metadata cache from the database.
        """

    def bootstrap(self) -> None:
        """Bootstrap handler.

        Is called when the new cluster is initialized (through ``initdb`` or a custom bootstrap method).
        """

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """Adjust GUCs in the current PostgreSQL configuration.

        :param parameters: dictionary of GUCs, with key as GUC name and corresponding value as current GUC value.
        """

    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        """Check whether provided replication *slot* existing in the database should not be removed.

        .. note::
            MPP database may create replication slots for its own use, for example to migrate data between workers
            using logical replication, and we don't want to suddenly drop them.

        :param slot: dictionary containing the replication slot settings, like ``name``, ``database``, ``type``, and
                     ``plugin``.

        :returns: always ``False``.
        """
        return False


def iter_mpp_classes(
        config: Optional[Union['Config', Dict[str, Any]]] = None
) -> Iterator[Tuple[str, Type[AbstractMPP]]]:
    """Attempt to import MPP modules that are present in the given configuration.

    :param config: configuration information with possible MPP names as keys. If given, only attempt to import MPP
                   modules defined in the configuration. Else, if ``None``, attempt to import any supported MPP module.

    :yields: tuples, each containing the module ``name`` and the imported MPP class object.
    """
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(__package__, str)
    yield from iter_classes(__package__, AbstractMPP, config)


def get_mpp(config: Union['Config', Dict[str, Any]]) -> AbstractMPP:
    """Attempt to load and instantiate a MPP module from known available implementations.

    :param config: object or dictionary with Patroni configuration.

    :returns: The successfully loaded MPP or fallback to :class:`Null`.
    """
    for name, mpp_class in iter_mpp_classes(config):
        if mpp_class.validate_config(config[name]):
            return mpp_class(config[name])
    return Null()
