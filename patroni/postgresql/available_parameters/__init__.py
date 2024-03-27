import logging
import os
import sys

from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

if sys.version_info < (3, 9):
    PathLikeObj = Path

    def get_validator_files() -> Iterator[PathLikeObj]:
        """Recursively find YAML files from the current package directory.

        :yields: :class:`Path` objects representing validator files.
        """
        conf_dir = Path(__file__).parent

        for root, _, filenames in os.walk(conf_dir):
            files = (Path(root) / filename for filename in filenames)
            for file in _filter_and_sort_files(files):
                yield file

else:
    from importlib.resources import files

    if sys.version_info < (3, 11):  # pragma: no cover
        from importlib.abc import Traversable
    else:  # pragma: no cover
        from importlib.resources.abc import Traversable

    PathLikeObj = Traversable

    def get_validator_files() -> Iterator[PathLikeObj]:
        """Recursively yield Traversable objects representing validator files from current direcotory."""

        def _traversable_walk(tvbs: Iterator[PathLikeObj]) -> Iterator[PathLikeObj]:
            for tvb in _filter_and_sort_files(tvbs):
                if tvb.is_file():
                    yield tvb
                elif tvb.is_dir():
                    yield from _traversable_walk(tvb.iterdir())

        return _traversable_walk(files(__name__).iterdir())


def _filter_and_sort_files(files: Iterator[PathLikeObj]) -> Iterator[PathLikeObj]:
    """Sort files by name, and filter out non-YAML files and Python files.

    :param files: A list of files and/or directories to be filtered and sorted.

    :yields: filtered and sorted objects.
    """
    for file in sorted(files):
        if file.name.lower().endswith((".yml", ".yaml")) or file.is_dir():
            yield file
        elif not file.name.lower().endswith((".py", ".pyc")):
            logger.info("Ignored a non-YAML file found under `%s` directory: `%s`.", file, __name__.split('.')[-1])
