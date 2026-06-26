import unittest

from patroni.collections import CaseInsensitiveDict


class TestCaseInsensitiveDict(unittest.TestCase):

    def test_keys_preserve_last_seen_case(self):
        values = CaseInsensitiveDict({'a': 'b', 'A': 'B', 'MixedCase': 1})

        self.assertEqual(list(values), ['A', 'MixedCase'])
        self.assertEqual(list(values.keys()), ['A', 'MixedCase'])
        self.assertEqual(list(values.items()), [('A', 'B'), ('MixedCase', 1)])
