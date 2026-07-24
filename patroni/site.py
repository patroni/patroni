"""Cluster site handling."""
import abc

from typing import Optional


class ClusterSite(abc.ABC):
    """An abstract class that encapsulates all the ``site`` logic."""

    def __init__(self, site: Optional[str]) -> None:
        """Initialize the :class:`ClusterSite` object.

        :param site: string representing name of the site assigned to the current node.
            loaded from the local configuration.
        """
        self._site = site

    @property
    def site(self) -> Optional[str]:
        """Site configured, if any.

        :return: string representing name of the site assigned to the current node.
        """
        return self._site

    def reload_site(self, site: Optional[str]) -> None:
        """Load and set relevant site value from configuration.

        :param site: string representing name of the site assigned to the current node.
            loaded from the local configuration.
        """
        self._site = site
