import logging
import sys

from typing import Iterator

logger = logging.getLogger(__name__)

if sys.version_info < (3, 9):  # pragma: no cover
    from pathlib import Path

    PathLikeObj = Path
    conf_dir = Path(__file__).parent
else:
    from importlib.resources import files

    if sys.version_info < (3, 11):  # pragma: no cover
        from importlib.abc import Traversable
    else:  # pragma: no cover
        from importlib.resources.abc import Traversable

    PathLikeObj = Traversable
    conf_dir = files(__name__)


def get_validator_files() -> Iterator[PathLikeObj]:
    """Recursively find YAML files from the current package directory.

    :returns: an iterator of :class:`PathLikeObj` objects representing validator files.
    """
    return _traversable_walk(conf_dir.iterdir())


def _traversable_walk(tvbs: Iterator[PathLikeObj]) -> Iterator[PathLikeObj]:
    """Recursively walk through Path/Traversable objects, yielding all YAML files in deterministic order.

    :param tvbs: An iterator over :class:`PathLikeObj` objects, where each object is a file or directory
                 that potentially contains YAML files.

    :yields: :class:`PathLikeObj` objects representing YAML files found during the traversal.
    """
    for tvb in _filter_and_sort_files(tvbs):
        if tvb.is_file():
            yield tvb
        elif tvb.is_dir():
            try:
                yield from _traversable_walk(tvb.iterdir())
            except Exception as e:
                logger.debug("Can't list directory %s: %r", tvb, e)


def _filter_and_sort_files(files: Iterator[PathLikeObj]) -> Iterator[PathLikeObj]:
    """Sort files by name, and filter out non-YAML files and Python files.

    :param files: A list of files and/or directories to be filtered and sorted.

    :yields: filtered and sorted objects.
    """
    for file in sorted(files, key=lambda x: x.name):
        if file.name.lower().endswith((".yml", ".yaml")) or file.is_dir():
            yield file
        elif not file.name.lower().endswith((".py", ".pyc")):
            logger.info("Ignored a non-YAML file found under `%s` directory: `%s`.", __name__.split('.')[-1], file)
