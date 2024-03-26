import logging
import os
import sys
from pathlib import Path
from typing import Iterator, Union

logger = logging.getLogger(__name__)

if sys.version_info < (3, 9):
    PathLikeObj = Path

    def get_validator_files() -> Iterator[PathLikeObj]:
        """Recursively yield Path objects representing validator files from current directory."""
        conf_dir = Path(__file__).parent

        for root, _, filenames in os.walk(conf_dir):
            files = (Path(root) / filename for filename in filenames)
            for file in _filter_and_sort_files(files):
                yield file

else:
    from importlib.resources import files

    if sys.version_info < (3, 11):
        from importlib.abc import Traversable
    else:
        from importlib.resources.abc import Traversable

    PathLikeObj = Union[Path, Traversable]

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

    :param files: A list of file or directory to be filtered and sorted.
    :return: An iterator over the filtered and sorted list.
    """
    for file in sorted(files, key=lambda f: f.name.lower()):
        if file.name.lower().endswith((".yml", ".yaml")) or file.is_dir():
            yield file
        elif not file.name.lower().endswith((".py", ".pyc")):
            logger.info(
                "Ignored a non-YAML file found under `available_parameters` directory: `%s`.",
                file,
            )
