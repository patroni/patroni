"""Tags handling."""
import abc

from typing import Any, Dict, Optional


class Tags(abc.ABC):
    """An abstract class that encapsulates all the ``tags`` logic.

    Child classes that want to use provided facilities must implement ``tags`` abstract property.
    """

    @staticmethod
    def _filter_tags(tags: Dict[str, Any]) -> Dict[str, Any]:
        """Get tags configured for this node, if any.

        Handle both predefined Patroni tags and custom defined tags.

        .. note::
            A custom tag is any tag added to the configuration ``tags`` section that is not one of ``clonefrom``,
            ``nofailover``, ``noloadbalance`` or ``nosync``.

            For the Patroni predefined tags, the returning object will only contain them if they are enabled as they
            all are boolean values that default to disabled.

        :returns: a dictionary of tags set for this node. The key is the tag name, and the value is the corresponding
            tag value.
        """
        return {tag: value for tag, value in tags.items()
                if tag not in ('clonefrom', 'nofailover', 'noloadbalance', 'nosync') or value}

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
        """``True`` if ``nofailover`` is ``True``, else ``False``."""
        return bool(self.tags.get('nofailover', False))

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
