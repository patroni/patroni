import unittest
import stat
from unittest.mock import Mock, patch

from patroni.file_perm import pg_perm


class TestFilePermissions(unittest.TestCase):

    @patch('os.stat')
    @patch('os.umask')
    @patch('patroni.file_perm.logger.error')
    def test_set_umask(self, mock_logger, mock_umask, mock_stat):
        mock_umask.side_effect = Exception
        mock_stat.return_value.st_mode = stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP
        pg_perm.set_permissions_from_data_directory('test')

        # umask is called with PG_MODE_MASK_GROUP
        self.assertEqual(mock_umask.call_args[0][0], stat.S_IWGRP | stat.S_IRWXO)
        self.assertEqual(mock_logger.call_args[0][0], 'Can not set umask to %03o: %r')

        mock_umask.reset_mock()
        mock_stat.return_value.st_mode = stat.S_IRWXU
        pg_perm.set_permissions_from_data_directory('test')
        # umask is called with PG_MODE_MASK_OWNER (permissions changed from group to owner)
        self.assertEqual(mock_umask.call_args[0][0], stat.S_IRWXG | stat.S_IRWXO)

    @patch('os.stat', Mock(side_effect=FileNotFoundError))
    @patch('patroni.file_perm.logger.error')
    def test_set_permissions_from_data_directory(self, mock_logger):
        pg_perm.set_permissions_from_data_directory('test')
        self.assertEqual(mock_logger.call_args[0][0], 'Can not check permissions on %s: %r')
