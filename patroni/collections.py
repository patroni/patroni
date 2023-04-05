from collections import OrderedDict
from collections.abc import MutableMapping, MutableSet
from typing import Any, Collection, Dict, Iterable, Iterator, Optional, Tuple, Union


class CaseInsensitiveSet(MutableSet):
    """A case-insensitive ``set``-like object.

    Implements all methods and operations of :class:``MutableSet``. All values are expected to be strings.
    The structure remembers the case of the last value set, however, contains testing is case insensitive.
    """
    def __init__(self, values: Optional[Collection[str]] = None) -> None:
        self._values = {}
        for v in values or ():
            self.add(v)

    def __repr__(self) -> str:
        return '<{0}{1} at {2:x}>'.format(type(self).__name__, tuple(self._values.values()), id(self))

    def __str__(self) -> str:
        return str(set(self._values.values()))

    def __contains__(self, value: str) -> bool:
        return value.lower() in self._values

    def __iter__(self) -> Iterator[str]:
        return iter(self._values.values())

    def __len__(self) -> int:
        return len(self._values)

    def add(self, value: str) -> None:
        self._values[value.lower()] = value

    def discard(self, value: str) -> None:
        self._values.pop(value.lower(), None)

    def issubset(self, other: 'CaseInsensitiveSet') -> bool:
        return self <= other


class CaseInsensitiveDict(MutableMapping):
    """A case-insensitive ``dict``-like object.

    Implements all methods and operations of :class:``MutableMapping`` as well as dict's :func:``copy``.
    All keys are expected to be strings. The structure remembers the case of the last key to be set,
    and ``iter(instance)``, ``keys()``, ``items()``, ``iterkeys()``, and ``iteritems()`` will contain
    case-sensitive keys. However, querying and contains testing is case insensitive.
    """
    def __init__(self, data: Optional[Union[Dict[str, Any], Iterable[Tuple[str, Any]]]] = None) -> None:
        self._values = OrderedDict()
        self.update(data or {})

    def __setitem__(self, key: str, value: Any) -> None:
        # Use the lowercase key for lookups, but store the actual key alongside the value.
        self._values[key.lower()] = (key, value)

    def __getitem__(self, key: str) -> Any:
        return self._values[key.lower()][1]

    def __delitem__(self, key: str) -> Any:
        del self._values[key.lower()]

    def __iter__(self) -> Iterator[str]:
        return iter(key for key, _ in self._values.values())

    def __len__(self) -> int:
        return len(self._values)

    def copy(self) -> 'CaseInsensitiveDict':
        return CaseInsensitiveDict(self._values.values())

    def __repr__(self) -> str:
        return '<{0}{1} at {2:x}>'.format(type(self).__name__, dict(self.items()), id(self))
