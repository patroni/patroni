"""Abstract class for Formation Cluster Handler."""
import abc
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union, Type

from ...dcs import Cluster
from ...exceptions import PatroniException
from ...dynamic_loader import iter_classes, iter_modules

if TYPE_CHECKING:  # pragma: no cover
    from .. import Postgresql
    from patroni.config import Config

logger = logging.getLogger(__name__)


def mpp_modules() -> List[str]:
    """Get names of MPP modules, depending on execution enviroment.

    :returns: list of know module names with absolute python module path namespace, e.g. `patroni.postgresql.mpp.citus`.
    """
    return iter_modules(__package__)


def iter_mpp_classes(
        config: Optional[Union['Config', Dict[str, Any]]] = None
) -> Iterator[Tuple[str, Type['AbstractMPP']]]:
    """Attempt to import MPP modules that are present in the given configuration.

    .. note::
            If a module successfully imports we can assume that all its requirements are installed.

    :param config: configuration information with possible MPP names as keys. If given, only attemp to import MPP
                   modules defined in the configuration. Else, if ``None``, attemp to import :class:`Null` module.

    :yields: a tuple containing the module ``name`` and the imported MPP class object.
    """
    return iter_classes(__package__, AbstractMPP, config)


def get_mpp(config: Union['Config', Dict[str, Any]]) -> 'AbstractMPP':
    """Attempt to load a MPP module from known available implementation.

    .. note::
        Using the list of available MPP classes returned by :func:`iter_mpp_classes` attempt to dynamically
        instantiate the class that implements a MPP using the abstract class :class:`AbstractMPP`.

        If no moudle is found to satisfy configuration the report and log an error. This will cause Patroni to exit.

    :returns: The successfully loaded MPP or fallback to :class:`Null`.
    """
    for name, mpp_class in iter_mpp_classes(config):
        if mpp_class.validate_config(config[name]):
            return mpp_class(config[name])

    return Null({})


class AbstractMPP(abc.ABC):

    group_re: Any   # re.Pattern[str]

    def __init__(self, config: Dict[str, Union[str, int]]) -> None:
        self._config = config

    def is_enabled(self) -> bool:
        """Check if MPP is enabled for a given MPP.

        .. note::
            We just check that the `_config` object isn't empty and expect it do be empty only in case of :class:`Null`.

        :returns: ``True`` if MPP is enabled, otherwise ``False``.
        """
        return bool(self._config)

    @staticmethod
    @abc.abstractmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for a given MPP.
        :returns: ``True`` is config passes validation, otherwise ``False``.
        """

    @property
    def group(self) -> Any:
        """The group for a given MPP implementation."""

    @property
    @abc.abstractmethod
    def coordinator_group_id(self) -> Any:
        """The groupid of the coordinator PostgreSQL cluster."""

    @property
    @abc.abstractmethod
    def mpp_type(self) -> str:
        """The type of the MPP cluster."""

    def is_coordinator(self) -> bool:
        """Check whether this node is running in the coordinator PostgreSQL cluster.

        :returns: ``True`` if MPP is enabled and the group id of this node
                  matches with the coordinator_group_id, otherwise ``False``.
        """
        return self.is_enabled() and self.group == self.coordinator_group_id

    def is_worker(self) -> bool:
        """Check whether this node is running in the worker PostgreSQL cluster.

        :returns: ``True`` if MPP is enabled this node is known to be not running
                  in the coordinator PostgreSQL cluster, otherwise ``False``.
        """
        return self.is_enabled() and not self.is_coordinator()

    def get_handler_impl(self, postgresql: 'Postgresql'):
        for cls in self.__class__.__subclasses__():
            if issubclass(cls, AbstractMPPHandler):
                return cls(postgresql, self._config)  # pylint: disable=abstract-class-instantiated # noqa: E0110
        raise PatroniException(f'Failed to initialize {self.__class__.__name__}Handler object')


class AbstractMPPHandler(AbstractMPP):
    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        super().__init__(config)
        self._postgresql = postgresql

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


class Null(AbstractMPP):

    @staticmethod
    def validate_config(config: Any) -> bool:
        return True

    @property
    def group(self) -> None:
        return None

    @property
    def coordinator_group_id(self) -> None:
        return None

    @property
    def mpp_type(self) -> str:
        return "null"


class NullHandler(Null, AbstractMPPHandler):
    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        AbstractMPPHandler.__init__(self, postgresql, config)

    def bootstrap(self) -> None:
        pass

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        pass

    def schedule_cache_rebuild(self) -> None:
        pass

    def sync_coordinator_meta(self, cluster: Cluster) -> None:
        pass

    def on_demote(self) -> None:
        pass

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        pass

    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        return False
