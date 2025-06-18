"""Helper functions to search for implementations of specific abstract interface in a package."""
import importlib
import inspect
import logging
import os
import pkgutil
import sys

from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type, TYPE_CHECKING, TypeVar, Union

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config

logger = logging.getLogger(__name__)


def iter_modules(package: str) -> List[str]:
    """Get names of modules from *package*, depending on execution environment.

    .. note::
        If being packaged with PyInstaller, modules aren't discoverable dynamically by scanning source directory because
        :class:`importlib.machinery.FrozenImporter` doesn't implement :func:`iter_modules`. But it is still possible to
        find all potential modules by iterating through ``toc``, which contains list of all "frozen" resources.

    :param package: a package name to search modules in, e.g. ``patroni.dcs``.

    :returns: list of known module names with absolute python module path namespace, e.g. ``patroni.dcs.etcd``.
    """
    module_prefix = package + '.'

    if getattr(sys, 'frozen', False):
        toc: Set[str] = set()
        # dirname may contain a few dots, which causes pkgutil.iter_importers()
        # to misinterpret the path as a package name. This can be avoided
        # altogether by not passing a path at all, because PyInstaller's
        # FrozenImporter is a singleton and registered as top-level finder.
        for importer in pkgutil.iter_importers():
            if hasattr(importer, 'toc'):
                toc |= getattr(importer, 'toc')
        # If it found the pyinstaller toc then use it, otherwise fall through to default
        # behavior which works in pyinstaller >= 4.4
        if len(toc) > 0:
            dots = module_prefix.count('.')  # search for modules only on the same level
            return [module for module in toc if module.startswith(module_prefix) and module.count('.') == dots]

    # here we are making an assumption that the package which is calling this function is already imported
    pkg_file = sys.modules[package].__file__
    if TYPE_CHECKING:  # pragma: no cover
        assert isinstance(pkg_file, str)
    return [name for _, name, is_pkg in pkgutil.iter_modules([os.path.dirname(pkg_file)], module_prefix) if not is_pkg]


ClassType = TypeVar("ClassType")


def find_class_in_module(module: ModuleType, cls_type: Type[ClassType]) -> Optional[Type[ClassType]]:
    """Try to find the implementation of *cls_type* class interface in *module* matching the *module* name.

    :param module: imported module.
    :param cls_type: a class type we are looking for.

    :returns: class with a name matching the name of *module* that implements *cls_type* or ``None`` if not found.
    """
    module_name = module.__name__.rpartition('.')[2]
    return next(
        (obj for obj_name, obj in module.__dict__.items()
         if (obj_name.lower() == module_name
             and inspect.isclass(obj) and issubclass(obj, cls_type))),
        None)


def iter_classes(
        package: str, cls_type: Type[ClassType],
        config: Optional[Union['Config', Dict[str, Any]]] = None
) -> Iterator[Tuple[str, Type[ClassType]]]:
    """Attempt to import modules and find implementations of *cls_type* that are present in the given configuration.

    .. note::
            If a module successfully imports we can assume that all its requirements are installed.

    :param package: a package name to search modules in, e.g. ``patroni.dcs``.
    :param cls_type: a class type we are looking for.
    :param config: configuration information with possible module names as keys. If given, only attempt to import
                   modules defined in the configuration. Else, if ``None``, attempt to import any supported module.

    :yields: a tuple containing the module ``name`` and the imported class object.
    """
    for mod_name in iter_modules(package):
        name = mod_name.rpartition('.')[2]
        if config is None or name in config:
            try:
                module = importlib.import_module(mod_name)
                module_cls = find_class_in_module(module, cls_type)
                if module_cls:
                    yield name, module_cls
            except ImportError:
                logger.log(logging.DEBUG if config is not None else logging.INFO,
                           'Failed to import %s', mod_name)
