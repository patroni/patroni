"""Tags handling."""
import abc

from typing import Any, Dict, Optional

from patroni.utils import parse_bool, parse_int


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
            The same rule applies for ``nosync`` and ``sync_priority`` tags.

        :returns: a dictionary of tags set for this node. The key is the tag name, and the value is the corresponding
            tag value.
        """
        return {tag: value for tag, value in tags.items()
                if any((tag not in ('clonefrom', 'nofailover', 'noloadbalance', 'nosync', 'nostream'),
                        value,
                        tag == 'nofailover' and 'failover_priority' in tags,
                        tag == 'nosync' and 'sync_priority' in tags))}

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

    def _priority_tag(self, bool_name: str, priority_name: str) -> int:
        """Common logic for obtaining the value of a priority tag from ``tags`` if defined.

        If boolean tag is defined as ``True``, this will return ``0``. Otherwise, it will return the value of
        the respective priority tag, defaulting to ``1`` if it's not defined or invalid.

        :param bool_name: name of the boolean tag (``nofailover``. ``nosync``).
        :param priority_name: name of the priority tag (``failover_priority``, ``sync_priority``).

        :returns: integer value based on the defined tags.
        """
        from_tags = self.tags.get(bool_name)
        priority = parse_int(self.tags.get(priority_name))
        priority = 1 if priority is None else priority
        return 0 if from_tags else priority

    def _bool_tag(self, bool_name: str, priority_name: str) -> bool:
        """Common logic for obtaining the value of a boolean tag from ``tags`` if defined.

        If boolean tag is not defined, this methods returns ``True`` if priority tag is non-positive,
        ``False`` otherwise.

        :param bool_name: name of the boolean tag (``nofailover``. ``nosync``).
        :param priority_name: name of the priority tag (``failover_priority``, ``sync_priority``).

        :returns: boolean value based on the defined tags.
        """
        from_tags = self.tags.get(bool_name)
        if from_tags is not None:
            # Value of bool tag takes precedence over priority tag
            return bool(from_tags)
        priority = parse_int(self.tags.get(priority_name))
        return priority is not None and priority <= 0

    @property
    def nofailover(self) -> bool:
        """``True`` if node configuration doesn't allow it to become primary, ``False`` otherwise."""
        return self._bool_tag('nofailover', 'failover_priority')

    @property
    def failover_priority(self) -> int:
        """Value of ``failover_priority`` from ``tags`` if defined, otherwise derived from ``nofailover``."""
        return self._priority_tag('nofailover', 'failover_priority')

    @property
    def noloadbalance(self) -> bool:
        """``True`` if ``noloadbalance`` is ``True``, else ``False``."""
        return bool(self.tags.get('noloadbalance', False))

    @property
    def nosync(self) -> bool:
        """``True`` if node configuration doesn't allow it to become synchronous, ``False`` otherwise."""
        return self._bool_tag('nosync', 'sync_priority')

    @property
    def sync_priority(self) -> int:
        """Value of ``sync_priority`` from ``tags`` if defined, otherwise derived from ``nosync``."""
        return self._priority_tag('nosync', 'sync_priority')

    @property
    def replicatefrom(self) -> Optional[str]:
        """Value of ``replicatefrom`` tag, if any."""
        return self.tags.get('replicatefrom')

    @property
    def nostream(self) -> bool:
        """``True`` if ``nostream`` is ``True``, else ``False``."""
        return parse_bool(self.tags.get('nostream')) or False
