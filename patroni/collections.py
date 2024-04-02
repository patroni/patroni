"""Patroni custom object types somewhat like :mod:`collections` module.

Provides a case insensitive :class:`dict` and :class:`set` object types, and `EMPTY_DICT` frozen dictionary object.
"""
from collections import OrderedDict
from copy import deepcopy
from typing import Any, Collection, Dict, Iterator, KeysView, Mapping, MutableMapping, MutableSet, Optional


class CaseInsensitiveSet(MutableSet[str]):
    """A case-insensitive :class:`set`-like object.

    Implements all methods and operations of :class:`~typing.MutableSet`. All values are expected to be strings.
    The structure remembers the case of the last value set, however, contains testing is case insensitive.
    """

    def __init__(self, values: Optional[Collection[str]] = None) -> None:
        """Create a new instance of :class:`CaseInsensitiveSet` with the given *values*.

        :param values: values to be added to the set.
        """
        self._values: Dict[str, str] = {}
        for v in values or ():
            self.add(v)

    def __repr__(self) -> str:
        """Get a string representation of the set.

        Provide a helpful way of recreating the set.

        :returns: representation of the set, showing its values.

        :Example:

            >>> repr(CaseInsensitiveSet(('1', 'test', 'Test', 'TESt', 'test2')))  # doctest: +ELLIPSIS
            "<CaseInsensitiveSet('1', 'TESt', 'test2') at ..."
        """
        return '<{0}{1} at {2:x}>'.format(type(self).__name__, tuple(self._values.values()), id(self))

    def __str__(self) -> str:
        """Get set values for printing.

        :returns: set of values in string format.

        :Example:

            >>> str(CaseInsensitiveSet(('1', 'test', 'Test', 'TESt', 'test2')))  # doctest: +SKIP
            "{'TESt', 'test2', '1'}"
        """
        return str(set(self._values.values()))

    def __contains__(self, value: object) -> bool:
        """Check if set contains *value*.

        The check is performed case-insensitively.

        :param value: value to be checked.

        :returns: ``True`` if *value* is already in the set, ``False`` otherwise.
        """
        return isinstance(value, str) and value.lower() in self._values

    def __iter__(self) -> Iterator[str]:
        """Iterate over the values in this set.

        :yields: values from set.
        """
        return iter(self._values.values())

    def __len__(self) -> int:
        """Get the length of this set.

        :returns: number of values in the set.

        :Example:

        >>> len(CaseInsensitiveSet(('1', 'test', 'Test', 'TESt', 'test2')))
        3
        """
        return len(self._values)

    def add(self, value: str) -> None:
        """Add *value* to this set.

        Search is performed case-insensitively. If *value* is already in the set, overwrite it with *value*, so we
        "remember" the last case of *value*.

        :param value: value to be added to the set.
        """
        self._values[value.lower()] = value

    def discard(self, value: str) -> None:
        """Remove *value* from this set.

        Search is performed case-insensitively. If *value* is not present in the set, no exception is raised.

        :param value: value to be removed from the set.
        """
        self._values.pop(value.lower(), None)

    def issubset(self, other: 'CaseInsensitiveSet') -> bool:
        """Check if this set is a subset of *other*.

        :param other: another set to be compared with this set.
        :returns: ``True`` if this set is a subset of *other*, else ``False``.
        """
        return self <= other


class CaseInsensitiveDict(MutableMapping[str, Any]):
    """A case-insensitive :class:`dict`-like object.

    Implements all methods and operations of :class:`~typing.MutableMapping` as well as :class:`dict`'s
    :func:`~dict.copy`. All keys are expected to be strings. The structure remembers the case of the last key to be set,
    and :func:`iter`, :func:`dict.keys`, :func:`dict.items`, :func:`dict.iterkeys`, and :func:`dict.iteritems` will
    contain case-sensitive keys. However, querying and contains testing is case insensitive.
    """

    def __init__(self, data: Optional[Dict[str, Any]] = None) -> None:
        """Create a new instance of :class:`CaseInsensitiveDict` with the given *data*.

        :param data: initial dictionary to create a :class:`CaseInsensitiveDict` from.
        """
        self._values: OrderedDict[str, Any] = OrderedDict()
        self.update(data or {})

    def __setitem__(self, key: str, value: Any) -> None:
        """Assign *value* to *key* in this dict.

        *key* is searched/stored case-insensitively in the dict. The corresponding value in the dict is a tuple of:
            * original *key*;
            * *value*.

        :param key: key to be created or updated in the dict.
        :param value: value for *key*.
        """
        self._values[key.lower()] = (key, value)

    def __getitem__(self, key: str) -> Any:
        """Get the value corresponding to *key*.

        *key* is searched case-insensitively in the dict.

        .. note:
            If *key* is not present in the dict, :class:`KeyError` will be triggered.

        :param key: key to be searched in the dict.

        :returns: value corresponding to *key*.
        """
        return self._values[key.lower()][1]

    def __delitem__(self, key: str) -> None:
        """Remove *key* from this dict.

        *key* is searched case-insensitively in the dict.

        .. note:
            If *key* is not present in the dict, :class:`KeyError` will be triggered.

        :param key: key to be removed from the dict.
        """
        del self._values[key.lower()]

    def __iter__(self) -> Iterator[str]:
        """Iterate over keys of this dict.

        :yields: each key present in the dict. Yields each key with its last case that has been stored.
        """
        return iter(key for key, _ in self._values.values())

    def __len__(self) -> int:
        """Get the length of this dict.

        :returns: number of keys in the dict.

        :Example:

        >>> len(CaseInsensitiveDict({'a': 'b', 'A': 'B', 'c': 'd'}))
        2
        """
        return len(self._values)

    def copy(self) -> 'CaseInsensitiveDict':
        """Create a copy of this dict.

        :return: a new dict object with the same keys and values of this dict.
        """
        return CaseInsensitiveDict({v[0]: v[1] for v in self._values.values()})

    def keys(self) -> KeysView[str]:
        """Return a new view of the dict's keys.

        :returns: a set-like object providing a view on the dict's keys
        """
        return self._values.keys()

    def __repr__(self) -> str:
        """Get a string representation of the dict.

        Provide a helpful way of recreating the dict.

        :returns: representation of the dict, showing its keys and values.

        :Example:

            >>> repr(CaseInsensitiveDict({'a': 'b', 'A': 'B', 'c': 'd'}))  # doctest: +ELLIPSIS
            "<CaseInsensitiveDict{'A': 'B', 'c': 'd'} at ..."
        """
        return '<{0}{1} at {2:x}>'.format(type(self).__name__, dict(self.items()), id(self))


class _FrozenDict(Mapping[str, Any]):
    """Frozen dictionary object."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new instance of :class:`_FrozenDict` with given data."""
        self.__values: Dict[str, Any] = dict(*args, **kwargs)

    def __iter__(self) -> Iterator[str]:
        """Iterate over keys of this dict.

        :yields: each key present in the dict. Yields each key with its last case that has been stored.
        """
        return iter(self.__values)

    def __len__(self) -> int:
        """Get the length of this dict.

        :returns: number of keys in the dict.

        :Example:

        >>> len(_FrozenDict())
        0
        """
        return len(self.__values)

    def __getitem__(self, key: str) -> Any:
        """Get the value corresponding to *key*.

        :returns: value corresponding to *key*.
        """
        return self.__values[key]

    def copy(self) -> Dict[str, Any]:
        """Create a copy of this dict.

        :return: a new dict object with the same keys and values of this dict.
        """
        return deepcopy(self.__values)


EMPTY_DICT = _FrozenDict()
