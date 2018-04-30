import unittest

from patroni.quorum import QuorumStateResolver, QuorumError

class QuorumTest(unittest.TestCase):
    def test_1111(self):
        state = 1, set("a"), 1, set("a")
        self.assertEquals(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=3)), [
            ('sync', 2, set('ab')),
            ('quorum', 1, set('ab')),
        ])
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abcde"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 3, set('abcde')),
            ('sync', 3, set('abcde')),
        ])

    def test_1222(self):
        """2 node cluster"""
        state = 1, set("ab"), 2, set("ab")
        # Active set matches state
        self.assertEquals(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=3)), [
        ])
        # Add node by increasing quorum
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)), [
            ('quorum', 2, set('abc')),
            ('sync', 2, set('abc')),
        ])
        # Add node by increasing sync
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 1, set('abc')),
        ])        
        # Add multiple nodes by increasing both sync and quorum
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abcde"), sync_wanted=3)), [
            ('sync', 3, set('abc')),
            ('quorum', 3, set('abcde')),
            ('sync', 3, set('abcde')),
        ])
        # Master is alone
        self.assertEquals(list(QuorumStateResolver(*state, active=set("a"), sync_wanted=2)), [
            ('quorum', 1, set('a')),
            ('sync', 1, set('a')),
        ])
        # Swap out sync replica
        self.assertEquals(list(QuorumStateResolver(*state, active=set("ac"), sync_wanted=2)), [
            ('quorum', 1, set('a')),
            ('sync', 2, set('ac')),
            ('quorum', 1, set('ac')),
        ])

    def test_1233(self):
        """Interrupted transition from 2 node cluster to 3 node fully sync cluster"""
        state = 1, set("ab"), 3, set("abc")
        # Node c went away, transition back to 2 node cluster
        self.assertEquals(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=3)), [
            ('sync', 2, set('ab')),
        ])
        # Node c is available transition to larger quorum set.
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)), [
            ('quorum', 1, set('abc')),
        ])
        # Add in a new node at the same time
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abcd"), sync_wanted=3)), [
            ('quorum', 2, set('abcd')),
            ('sync', 3, set('abcd')),
        ])
        # Change replication factor at the same time
        self.assertEquals(list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)), [
            ('quorum', 2, set('abc')),
            ('sync', 2, set('abc')),
        ])

    def test_2322(self):
        """Interrupted transition from 2 node cluster to 3 node cluster with replication factor 2"""
        state = 2, set("abc"), 2, set("ab")
        self.assertEquals(list(QuorumStateResolver(*state, active=set("ab"), sync_wanted=2)), [
            ('quorum', 1, set('ab')),
        ])

    def test_3535(self):
        state = 3, set("abcde"), 3, set("abcde")
        resolver = QuorumStateResolver(*state, active=set("abc"), sync_wanted=3)
        self.assertEquals(next(iter(resolver)),
            ('sync', 3, set('abc')),
        )
        resolver.active |= set('d')
        self.assertEquals(list(resolver), [
            ('quorum', 2, set('abcd')),
            ('sync', 3, set('abcd')),
        ])

    def test_nonsync_promotion(self):
        # Beginning state: 2 of abc in sync. e.g. (a master, ssn = ANY 1 (b c))
        # a fails, d sees b and c, knows that it is in sync and decides to promote.
        # We include in sync state everybody that already was in sync, increasing replication factor
        # and let situation resolve. Node d ssn=ANY 2 (a b c)
        state = 2, set("abc"), 3, set("abcd")
        resolver = QuorumStateResolver(*state, active=set("bcd"), sync_wanted=2)
        self.assertEquals(list(resolver), [
            ('quorum', 2, set('abcd')), # Set ourselves to be a member of the quorum
            ('sync', 3, set('bcd')),    # Remove a from being synced to.
            ('quorum', 2, set('bcd')),  # Remove a from quorum
            ('sync', 2, set('bcd')),    # Can now reduce replication factor to original value
        ])

    def test_invalid_states(self):
        # TODO: handle these cases
        # Main invariant is not satisfied, system is in an unsafe state
        state = 1, set("abc"), 2, set("abc")
        self.assertRaises(QuorumError, lambda:list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)))
        # Quorum and sync states mismatched, somebody other than Patroni modified system state
        state = 2, set("abc"), 3, set("abd")
        self.assertRaises(QuorumError, lambda:list(QuorumStateResolver(*state, active=set("abc"), sync_wanted=2)))
