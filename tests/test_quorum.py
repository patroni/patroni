import unittest

from typing import List, Set, Tuple

from patroni.quorum import QuorumStateResolver, QuorumError


class QuorumTest(unittest.TestCase):

    def check_state_transitions(self, leader: str, quorum: int, voters: Set[str], numsync: int, sync: Set[str],
                                numsync_confirmed: int, active: Set[str], sync_wanted: int, leader_wanted: str,
                                expected: List[Tuple[str, str, int, Set[str]]]) -> None:
        kwargs = {
            'leader': leader, 'quorum': quorum, 'voters': voters,
            'numsync': numsync, 'sync': sync, 'numsync_confirmed': numsync_confirmed,
            'active': active, 'sync_wanted': sync_wanted, 'leader_wanted': leader_wanted
        }
        result = list(QuorumStateResolver(**kwargs))
        self.assertEqual(result, expected)

        # also check interrupted transitions
        if len(result) > 0 and result[0][0] != 'restart' and kwargs['leader'] == result[0][1]:
            if result[0][0] == 'sync':
                kwargs.update(numsync=result[0][2], sync=result[0][3])
            else:
                kwargs.update(leader=result[0][1], quorum=result[0][2], voters=result[0][3])
            kwargs['expected'] = expected[1:]
            self.check_state_transitions(**kwargs)

    def test_1111(self):
        leader = 'a'

        # Add node
        self.check_state_transitions(leader=leader, quorum=0, voters=set(),
                                     numsync=0, sync=set(), numsync_confirmed=0, active=set('b'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 1, set('b')),
            ('restart', leader, 0, set()),
        ])
        self.check_state_transitions(leader=leader, quorum=0, voters=set(),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('b'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('b'))
        ])

        self.check_state_transitions(leader=leader, quorum=0, voters=set(),
                                     numsync=0, sync=set(), numsync_confirmed=0, active=set('bcde'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bcde')),
            ('restart', leader, 0, set()),
        ])
        self.check_state_transitions(leader=leader, quorum=0, voters=set(),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=1, active=set('bcde'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 3, set('bcde')),
        ])

    def test_1222(self):
        """2 node cluster"""
        leader = 'a'

        # Active set matches state
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('b'),
                                     sync_wanted=2, leader_wanted=leader, expected=[])

        # Add node by increasing quorum
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('BC'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 1, set('bC')),
            ('sync', leader, 1, set('bC')),
        ])

        # Add node by increasing sync
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('bc'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 1, set('bc')),
        ])
        # Reduce quorum after added node caught up
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bc'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=2, active=set('bc'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('bc')),
        ])

        # Add multiple nodes by increasing both sync and quorum
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('BCdE'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bC')),
            ('quorum', leader, 3, set('bCdE')),
            ('sync', leader, 2, set('bCdE')),
        ])
        # Reduce quorum after added nodes caught up
        self.check_state_transitions(leader=leader, quorum=3, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=3, active=set('bcde'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 2, set('bcde')),
        ])

        # Primary is alone
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=1, sync=set('b'), numsync_confirmed=0, active=set(),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set()),
            ('sync', leader, 0, set()),
        ])

        # Swap out sync replica
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=1, sync=set('b'), numsync_confirmed=0, active=set('c'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set()),
            ('sync', leader, 1, set('c')),
            ('restart', leader, 0, set()),
        ])
        # Update quorum when added node caught up
        self.check_state_transitions(leader=leader, quorum=0, voters=set(),
                                     numsync=1, sync=set('c'), numsync_confirmed=1, active=set('c'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('c')),
        ])

    def test_1233(self):
        """Interrupted transition from 2 node cluster to 3 node fully sync cluster"""
        leader = 'a'

        # Node c went away, transition back to 2 node cluster
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=1, active=set('b'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 1, set('b')),
        ])

        # Node c is available transition to larger quorum set, but not yet caught up.
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=1, active=set('bc'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 1, set('bc')),
        ])

        # Add in a new node at the same time, but node c didn't caught up yet
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=1, active=set('bcd'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 2, set('bcd')),
            ('sync', leader, 2, set('bcd')),
        ])
        # All sync nodes caught up, reduce quorum
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcd'),
                                     numsync=2, sync=set('bcd'), numsync_confirmed=3, active=set('bcd'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 1, set('bcd')),
        ])

        # Change replication factor at the same time
        self.check_state_transitions(leader=leader, quorum=0, voters=set('b'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=1, active=set('bc'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 1, set('bc')),
            ('sync', leader, 1, set('bc')),
        ])

    def test_2322(self):
        """Interrupted transition from 2 node cluster to 3 node cluster with replication factor 2"""
        leader = 'a'

        # Node c went away, transition back to 2 node cluster
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bc'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('b'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('b')),
        ])

        # Node c is available transition to larger quorum set.
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bc'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('bc'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('sync', leader, 1, set('bc')),
        ])

        # Add in a new node at the same time
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bc'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('bcd'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('sync', leader, 1, set('bc')),
            ('quorum', leader, 2, set('bcd')),
            ('sync', leader, 1, set('bcd')),
        ])

        # Convert to a fully synced cluster
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bc'),
                                     numsync=1, sync=set('b'), numsync_confirmed=1, active=set('bc'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bc')),
        ])
        # Reduce quorum after all nodes caught up
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bc'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=2, active=set('bc'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('bc')),
        ])

    def test_3535(self):
        leader = 'a'

        # remove nodes
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=2, active=set('bc'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 0, set('bc')),
        ])
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=3, active=set('bcd'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bcd')),
            ('quorum', leader, 1, set('bcd')),
        ])

        # remove nodes and decrease sync
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=2, active=set('bc'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bc')),
            ('quorum', leader, 1, set('bc')),
            ('sync', leader, 1, set('bc')),
        ])
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bcde'),
                                     numsync=3, sync=set('bcde'), numsync_confirmed=2, active=set('bc'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('sync', leader, 3, set('bcd')),
            ('quorum', leader, 1, set('bc')),
            ('sync', leader, 1, set('bc')),
        ])

        # Increase replication factor and decrease quorum
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=2, active=set('bcde'),
                                     sync_wanted=3, leader_wanted=leader, expected=[
            ('sync', leader, 3, set('bcde')),
        ])
        # decrease quorum after more nodes caught up
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=3, sync=set('bcde'), numsync_confirmed=3, active=set('bcde'),
                                     sync_wanted=3, leader_wanted=leader, expected=[
            ('quorum', leader, 1, set('bcde')),
        ])

        # Add node with decreasing sync and increasing quorum
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=2, active=set('bcdef'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            # increase quorum by 2, 1 for added node and another for reduced sync
            ('quorum', leader, 4, set('bcdef')),
            # now reduce replication factor to requested value
            ('sync', leader, 1, set('bcdef')),
        ])

        # Remove node with increasing sync and decreasing quorum
        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcde'),
                                     numsync=2, sync=set('bcde'), numsync_confirmed=2, active=set('bcd'),
                                     sync_wanted=3, leader_wanted=leader, expected=[
            # node e removed from sync wth replication factor increase
            ('sync', leader, 3, set('bcd')),
            # node e removed from voters with quorum decrease
            ('quorum', leader, 1, set('bcd')),
        ])

    def test_remove_nosync_node(self):
        leader = 'a'
        self.check_state_transitions(leader=leader, quorum=0, voters=set('bc'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=1, active=set('b'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('b')),
            ('sync', leader, 1, set('b'))
        ])

    def test_swap_sync_node(self):
        leader = 'a'
        self.check_state_transitions(leader=leader, quorum=0, voters=set('bc'),
                                     numsync=2, sync=set('bc'), numsync_confirmed=1, active=set('bd'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set('b')),
            ('sync', leader, 2, set('bd')),
            ('quorum', leader, 1, set('bd'))
        ])

    def test_promotion(self):
        # Beginning stat: 'a' in the primary, 1 of bcd in sync
        # a fails, c gets quorum votes and promotes
        self.check_state_transitions(leader='a', quorum=2, voters=set('bcd'),
                                     numsync=0, sync=set(), numsync_confirmed=0, active=set(),
                                     sync_wanted=1, leader_wanted='c', expected=[
            ('sync', 'a', 1, set('abd')),    # set a and b to sync
            ('quorum', 'c', 2, set('abd')),  # set c as a leader and move a to voters
            #  and stop because there are no active nodes
        ])

        # next loop, b managed to reconnect
        self.check_state_transitions(leader='c', quorum=2, voters=set('abd'),
                                     numsync=1, sync=set('abd'), numsync_confirmed=0, active=set('b'),
                                     sync_wanted=1, leader_wanted='c', expected=[
            ('sync', 'c', 1, set('b')),    # remove a from sync as inactive
            ('quorum', 'c', 0, set('b')),  # remove a from voters and reduce quorum
        ])

        # alternative reality: next loop, no one reconnected
        self.check_state_transitions(leader='c', quorum=2, voters=set('abd'),
                                     numsync=1, sync=set('abd'), numsync_confirmed=0, active=set(),
                                     sync_wanted=1, leader_wanted='c', expected=[
            ('quorum', 'c', 0, set()),
            ('sync', 'c', 0, set()),
        ])

    def test_nonsync_promotion(self):
        # Beginning state: 1 of bc in sync. e.g. (a primary, ssn = ANY 1 (b c))
        # a fails, d sees b and c, knows that it is in sync and decides to promote.
        # We include in sync state former primary increasing replication factor
        # and let situation resolve. Node d ssn=ANY 1 (b c)
        leader = 'd'
        self.check_state_transitions(leader='a', quorum=1, voters=set('bc'),
                                     numsync=0, sync=set(), numsync_confirmed=0, active=set(),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            # Set a, b, and c to sync and increase replication factor
            ('sync', 'a', 2, set('abc')),
            # Set ourselves as the leader and move the old leader to voters
            ('quorum', leader, 1, set('abc')),
            # and stop because there are no active nodes
        ])
        # next loop, b and c managed to reconnect
        self.check_state_transitions(leader=leader, quorum=1, voters=set('abc'),
                                     numsync=2, sync=set('abc'), numsync_confirmed=0, active=set('bc'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('sync', leader, 2, set('bc')),     # Remove a from being synced to.
            ('quorum', leader, 1, set('bc')),   # Remove a from quorum
            ('sync', leader, 1, set('bc')),     # Can now reduce replication factor back
        ])
        # alternative reality: next loop, no one reconnected
        self.check_state_transitions(leader=leader, quorum=1, voters=set('abc'),
                                     numsync=2, sync=set('abc'), numsync_confirmed=0, active=set(),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            ('quorum', leader, 0, set()),
            ('sync', leader, 0, set()),
        ])

    def test_invalid_states(self):
        leader = 'a'

        # Main invariant is not satisfied, system is in an unsafe state
        resolver = QuorumStateResolver(leader=leader, quorum=0, voters=set('bc'),
                                       numsync=1, sync=set('bc'), numsync_confirmed=1,
                                       active=set('bc'), sync_wanted=1, leader_wanted=leader)
        self.assertRaises(QuorumError, resolver.check_invariants)
        self.assertEqual(list(resolver), [
            ('quorum', leader, 1, set('bc'))
        ])

        # Quorum and sync states mismatched, somebody other than Patroni modified system state
        resolver = QuorumStateResolver(leader=leader, quorum=1, voters=set('bc'),
                                       numsync=2, sync=set('bd'), numsync_confirmed=1,
                                       active=set('bd'), sync_wanted=1, leader_wanted=leader)
        self.assertRaises(QuorumError, resolver.check_invariants)
        self.assertEqual(list(resolver), [
            ('quorum', leader, 1, set('bd')),
            ('sync', leader, 1, set('bd')),
        ])
        self.assertTrue(repr(resolver.sync).startswith('<CaseInsensitiveSet'))

    def test_sync_high_quorum_low_safety_margin_high(self):
        leader = 'a'

        self.check_state_transitions(leader=leader, quorum=2, voters=set('bcdef'),
                                     numsync=4, sync=set('bcdef'), numsync_confirmed=3, active=set('bcdef'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            ('quorum', leader, 3, set('bcdef')),  # Adjust quorum requirements
            ('sync', leader, 2, set('bcdef')),    # Reduce synchronization
        ])

    def test_quorum_update(self):
        resolver = QuorumStateResolver(leader='a', quorum=1, voters=set('bc'), numsync=1, sync=set('bc'),
                                       numsync_confirmed=1, active=set('bc'), sync_wanted=1, leader_wanted='a')
        self.assertRaises(QuorumError, list, resolver.quorum_update(-1, set()))
        self.assertRaises(QuorumError, list, resolver.quorum_update(1, set()))

    def test_sync_update(self):
        resolver = QuorumStateResolver(leader='a', quorum=1, voters=set('bc'), numsync=1, sync=set('bc'),
                                       numsync_confirmed=1, active=set('bc'), sync_wanted=1, leader_wanted='a')
        self.assertRaises(QuorumError, list, resolver.sync_update(-1, set()))
        self.assertRaises(QuorumError, list, resolver.sync_update(1, set()))

    def test_remove_nodes_with_decreasing_sync(self):
        leader = 'a'

        # Remove node with decreasing sync
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bcdef'),
                                     numsync=4, sync=set('bcdef'), numsync_confirmed=2, active=set('bcd'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            # node f removed from sync
            ('sync', leader, 4, set('bcde')),
            # nodes e and f removed from voters with quorum decrease
            ('quorum', leader, 1, set('bcd')),
            # node e removed from sync with replication factor decrease
            ('sync', leader, 2, set('bcd')),
        ])

        # Interrupted state, and node g joined
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bcdef'),
                                     numsync=4, sync=set('bcde'), numsync_confirmed=2, active=set('bcdg'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            # remove nodes e and f from voters
            ('quorum', leader, 1, set('bcd')),
            # remove node e from sync and reduce replication factor
            ('sync', leader, 3, set('bcd')),
            # add node g to voters with quorum increase
            ('quorum', leader, 2, set('bcdg')),
            # add node g to sync and reduce replication factor
            ('sync', leader, 2, set('bcdg')),
        ])

        # node f returned
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bcdef'),
                                     numsync=4, sync=set('bcde'), numsync_confirmed=2, active=set('bcdf'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            # replace node e with f in sync
            ('sync', leader, 4, set('bcdf')),
            # remove nodes e from voters with quorum decrease
            ('quorum', leader, 2, set('bcdf')),
            # reduce replication factor as it was requested
            ('sync', leader, 2, set('bcdf')),
        ])

        # node e returned
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bcdef'),
                                     numsync=4, sync=set('bcde'), numsync_confirmed=2, active=set('bcde'),
                                     sync_wanted=2, leader_wanted=leader, expected=[
            # remove nodes f from voters with quorum decrease
            ('quorum', leader, 2, set('bcde')),
            # reduce replication factor as it was requested
            ('sync', leader, 2, set('bcde')),
        ])

        # node b is also lost
        self.check_state_transitions(leader=leader, quorum=1, voters=set('bcdef'),
                                     numsync=4, sync=set('bcde'), numsync_confirmed=2, active=set('cd'),
                                     sync_wanted=1, leader_wanted=leader, expected=[
            # remove nodes b, e, and f from voters
            ('quorum', leader, 1, set('cd')),
            # remove nodes b and e from sync with replication factor decrease
            ('sync', leader, 1, set('cd')),
        ])

    def test_empty_ssn(self):
        # Beginning stat: 'a' in the primary, 1 of bc in sync
        # a fails, c gets quorum votes and promotes
        self.check_state_transitions(leader='a', quorum=1, voters=set('bc'),
                                     numsync=1, sync=set(), numsync_confirmed=0, active=set(),
                                     sync_wanted=1, leader_wanted='c', expected=[
            ('sync', 'a', 1, set('ab')),    # remove a from sync as inactive
            ('quorum', 'c', 1, set('ab')),  # set c as a leader and move a to voters
            #  and stop because there are no active nodes
        ])

        # next loop, b managed to reconnect
        self.check_state_transitions(leader='c', quorum=1, voters=set('ab'),
                                     numsync=1, sync=set('ab'), numsync_confirmed=0, active=set('b'),
                                     sync_wanted=1, leader_wanted='c', expected=[
            ('sync', 'c', 1, set('b')),    # remove a from sync as inactive
            ('quorum', 'c', 0, set('b')),  # remove a from voters and reduce quorum
        ])
