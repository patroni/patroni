"""Tags handling."""
import abc

from typing import Any, Dict, Optional

from patroni.utils import parse_int, parse_bool


class Tags(abc.ABC):
    """An abstract class that encapsulates all the ``tags`` logic.

    Child classes that want to use provided facilities must implement ``tags`` abstract property.

    .. note::
        Due to backward-compatibility reasons, old tags may have a less strict type conversion than new ones.
    """

    @staticmethod
    def _filter_tags(tags: Dict[str, Any]) -> Dict[str, Any]:
        """Get tags configured for this node, if any.

        Handle both predefined Patroni tags and custom defined tags.

        .. note::
            A custom tag is any tag added to the configuration ``tags`` section that is not one of ``clonefrom``,
            ``nofailover``, ``noloadbalance``,``nosync`` or ``nostream``.

            For most of the Patroni predefined tags, the returning object will only contain them if they are enabled as
            they all are boolean values that default to disabled.
            However ``nofailover`` tag is always returned if ``failover_priority`` tag is defined. In this case, we need
            both values to see if they are contradictory and the ``nofailover`` value should be used.

        :returns: a dictionary of tags set for this node. The key is the tag name, and the value is the corresponding
            tag value.
        """
        return {tag: value for tag, value in tags.items()
                if any((tag not in ('clonefrom', 'nofailover', 'noloadbalance', 'nosync', 'nostream'),
                        value,
                        tag == 'nofailover' and 'failover_priority' in tags))}

    @property
    @abc.abstractmethod
    def tags(self) -> Dict[str, Any]:
        """Configured tags.

        Must be implemented in a child class.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def clonefrom(self) -> bool:
        """``True`` if ``clonefrom`` tag is ``True``, else ``False``."""
        return self.tags.get('clonefrom', False)

    @property
    def nofailover(self) -> bool:
        """Common logic for obtaining the value of ``nofailover`` from ``tags`` if defined.

        If ``nofailover`` is not defined, this methods returns ``True`` if ``failover_priority`` is non-positive,
        ``False`` otherwise.
        """
        from_tags = self.tags.get('nofailover')
        if from_tags is not None:
            # Value of `nofailover` takes precedence over `failover_priority`
            return bool(from_tags)
        failover_priority = parse_int(self.tags.get('failover_priority'))
        return failover_priority is not None and failover_priority <= 0

    @property
    def failover_priority(self) -> int:
        """Common logic for obtaining the value of ``failover_priority`` from ``tags`` if defined.

        If ``nofailover`` is defined as ``True``, this will return ``0``. Otherwise, it will return the value of
        ``failover_priority``, defaulting to ``1`` if it's not defined or invalid.
        """
        from_tags = self.tags.get('nofailover')
        failover_priority = parse_int(self.tags.get('failover_priority'))
        failover_priority = 1 if failover_priority is None else failover_priority
        return 0 if from_tags else failover_priority

    @property
    def noloadbalance(self) -> bool:
        """``True`` if ``noloadbalance`` is ``True``, else ``False``."""
        return bool(self.tags.get('noloadbalance', False))

    @property
    def nosync(self) -> bool:
        """``True`` if ``nosync`` is ``True``, else ``False``."""
        return bool(self.tags.get('nosync', False))

    @property
    def replicatefrom(self) -> Optional[str]:
        """Value of ``replicatefrom`` tag, if any."""
        return self.tags.get('replicatefrom')

    @property
    def nostream(self) -> bool:
        """``True`` if ``nostream`` is ``True``, else ``False``."""
        return parse_bool(self.tags.get('nostream')) or False
