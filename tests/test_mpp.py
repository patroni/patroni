from typing import Any
from patroni.exceptions import PatroniException
from patroni.postgresql.mpp import AbstractMPP, get_mpp, Null

from . import BaseTestPostgresql
from .test_ha import get_cluster_initialized_with_leader


class TestMPP(BaseTestPostgresql):

    def setUp(self):
        super(TestMPP, self).setUp()
        self.cluster = get_cluster_initialized_with_leader()

    def test_get_handler_impl_exception(self):
        class DummyMPP(AbstractMPP):
            def __init__(self) -> None:
                super().__init__({})

            @staticmethod
            def validate_config(config: Any) -> bool:
                return True

            @property
            def group(self) -> None:
                return None

            @property
            def coordinator_group_id(self) -> None:
                return None

            @property
            def type(self) -> str:
                return "dummy"

        mpp = DummyMPP()
        self.assertRaises(PatroniException, mpp.get_handler_impl, self.p)

    def test_null_handler(self):
        config = {}
        mpp = get_mpp(config)
        self.assertIsInstance(mpp, Null)
        self.assertIsNone(mpp.group)
        self.assertTrue(mpp.validate_config(config))
        nullHandler = mpp.get_handler_impl(self.p)
        self.assertIsNone(nullHandler.handle_event(self.cluster, {}))
        self.assertIsNone(nullHandler.sync_meta_data(self.cluster))
        self.assertIsNone(nullHandler.on_demote())
        self.assertIsNone(nullHandler.schedule_cache_rebuild())
        self.assertIsNone(nullHandler.bootstrap())
        self.assertIsNone(nullHandler.adjust_postgres_gucs({}))
        self.assertFalse(nullHandler.ignore_replication_slot({}))
