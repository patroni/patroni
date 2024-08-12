"""Helper object that helps with figuring out file and directory permissions based on permissions of PGDATA.

:var logger: logger of this module.
:var pg_perm: instance of the :class:`__FilePermissions` object.
"""
import logging
import os
import stat

logger = logging.getLogger(__name__)


class __FilePermissions:
    """Helper class for managing permissions of directories and files under PGDATA.

    Execute :meth:`set_permissions_from_data_directory` to figure out which permissions should be used for files and
    directories under PGDATA based on permissions of PGDATA root directory.
    """

    # Mode mask for data directory permissions that only allows the owner to
    # read/write directories and files -- mask 077.
    __PG_MODE_MASK_OWNER = stat.S_IRWXG | stat.S_IRWXO

    # Mode mask for data directory permissions that also allows group read/execute -- mask 027.
    __PG_MODE_MASK_GROUP = stat.S_IWGRP | stat.S_IRWXO

    # Default mode for creating directories -- mode 700.
    __PG_DIR_MODE_OWNER = stat.S_IRWXU

    # Mode for creating directories that allows group read/execute -- mode 750.
    __PG_DIR_MODE_GROUP = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP

    # Default mode for creating files -- mode 600.
    __PG_FILE_MODE_OWNER = stat.S_IRUSR | stat.S_IWUSR

    # Mode for creating files that allows group read -- mode 640.
    __PG_FILE_MODE_GROUP = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP

    def __init__(self) -> None:
        """Create a :class:`__FilePermissions` object and set default permissions."""
        self.__set_owner_permissions()
        self.__orig_umask = self.__set_umask()

    def __set_umask(self) -> int:
        """Set umask value based on calculations.

        .. note::
            Should only be called once either :meth:`__set_owner_permissions`
            or :meth:`__set_group_permissions` has been executed.

        :returns: the previous value of the umask or ``0022`` if umask call failed.
        """
        try:
            return os.umask(self.__pg_mode_mask)
        except Exception as e:
            logger.error('Can not set umask to %03o: %r', self.__pg_mode_mask, e)
            return 0o22

    @property
    def orig_umask(self) -> int:
        """Original umask value."""
        return self.__orig_umask

    def __set_owner_permissions(self) -> None:
        """Make directories/files accessible only by the owner."""
        self.__pg_dir_create_mode = self.__PG_DIR_MODE_OWNER
        self.__pg_file_create_mode = self.__PG_FILE_MODE_OWNER
        self.__pg_mode_mask = self.__PG_MODE_MASK_OWNER

    def __set_group_permissions(self) -> None:
        """Make directories/files accessible by the owner and readable by group."""
        self.__pg_dir_create_mode = self.__PG_DIR_MODE_GROUP
        self.__pg_file_create_mode = self.__PG_FILE_MODE_GROUP
        self.__pg_mode_mask = self.__PG_MODE_MASK_GROUP

    def set_permissions_from_data_directory(self, data_dir: str) -> None:
        """Set new permissions based on provided *data_dir*.

        :param data_dir: reference to PGDATA to calculate permissions from.
        """
        try:
            st = os.stat(data_dir)
            if (st.st_mode & self.__PG_DIR_MODE_GROUP) == self.__PG_DIR_MODE_GROUP:
                self.__set_group_permissions()
            else:
                self.__set_owner_permissions()
        except Exception as e:
            logger.error('Can not check permissions on %s: %r', data_dir, e)
        else:
            self.__set_umask()

    @property
    def dir_create_mode(self) -> int:
        """Directory permissions."""
        return self.__pg_dir_create_mode

    @property
    def file_create_mode(self) -> int:
        """File permissions."""
        return self.__pg_file_create_mode


pg_perm = __FilePermissions()
