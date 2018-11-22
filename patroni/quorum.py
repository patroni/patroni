import logging

logger = logging.getLogger(__name__)

def clamp(value, min=None, max=None):
    if min is not None and value < min:
        value = min
    if max is not None and value > max:
        value = max
    return value


class QuorumError(Exception):
    pass


class QuorumStateResolver(object):
    def __init__(self, quorum, voters, numsync, sync, active, sync_wanted):
        self.quorum = quorum
        self.voters = set(voters)
        self.numsync = numsync
        self.sync = set(sync)
        self.active = active
        self.sync_wanted = sync_wanted

    def check_invariants(self):
        if self.quorum and not (len(self.voters|self.sync) < self.quorum + self.numsync):
            raise QuorumError("Quorum and sync not guaranteed to overlap: nodes %d >= quorum %d + sync %d" %
                              (len(self.voters|self.sync), self.quorum, self.numsync))
        if not (self.voters <= self.sync or self.sync <= self.voters):
            raise QuorumError("Mismatched sets: quorum only=%s sync only=%s" %
                              (self.voters - self.sync, self.sync - self.voters))

    def quorum_update(self, quorum, voters):
        if quorum < 1:
            raise QuorumError("Quorum %d < 0 of (%s)" % (quorum, voters))
        self.quorum = quorum
        self.voters = voters
        self.check_invariants()
        return 'quorum', self.quorum, self.voters

    def sync_update(self, numsync, sync):
        self.numsync = numsync
        self.sync = sync
        self.check_invariants()
        return 'sync', self.numsync, self.sync

    def __iter__(self):
        transitions = list(self._generate_transitions())
        # Merge 2 transitions of the same type to a single one. This is always safe because skipping the first
        # transition is equivalent to no one observing the intermediate state.
        for cur_transition, next_transition in zip(transitions, transitions[1:]+[None]):
            if next_transition and cur_transition[0] == next_transition[0]:
                continue
            yield cur_transition

    def _generate_transitions(self):
        self.check_invariants()

        # Handle non steady state cases
        if self.sync < self.voters:
            logger.info("Case 1")
            # Case 1: quorum is superset of sync nodes. In the middle of changing quorum.
            # Evict from quorum dead nodes that are not being synced.
            remove_from_quorum = self.voters - (self.sync | self.active)
            if remove_from_quorum:
                yield self.quorum_update(
                    quorum=len(self.voters) - len(remove_from_quorum) + 1 - self.numsync,
                    voters=self.voters - remove_from_quorum)
            # Start syncing to nodes that are in quorum and alive
            add_to_sync = self.voters - self.sync
            if add_to_sync:
                yield self.sync_update(self.numsync, self.sync | add_to_sync)
        elif self.sync > self.voters:
            logger.info("Case 2")
            # Case 2: sync is superset of quorum nodes. In the middle of changing replication factor.
            # Add to quorum voters nodes that are already synced and active
            add_to_quorum = (self.sync - self.voters) & self.active
            if add_to_quorum:
                yield self.quorum_update(
                        quorum=self.quorum,
                        voters=self.voters | add_to_quorum)
            # Remove from sync nodes that are dead
            remove_from_sync = self.sync - self.voters
            if remove_from_sync:
                yield self.sync_update(
                        numsync=min(self.sync_wanted, len(self.sync) - len(remove_from_sync)),
                        sync=self.sync - remove_from_sync)

        # After handling these two cases quorum and sync must match.
        assert self.voters == self.sync

        safety_margin = self.quorum + self.numsync - len(self.voters|self.sync)
        if safety_margin > 1:
            logger.info("Case 3")
            # Case 3: quorum or replication factor is bigger than needed. In the middle of changing requested replication factor.
            if self.numsync > self.sync_wanted:
                # Reduce replication factor
                yield self.sync_update(min(self.sync_wanted, len(self.sync)), self.sync)
            elif len(self.voters) > self.numsync:
                # Reduce quorum
                yield self.quorum_update(len(self.voters) + 1 - self.numsync, self.voters)

        # We are in a steady state point. Find if desired state is different and act accordingly.
        logger.info("Steady state")

        # If any nodes have gone away, evict them
        to_remove = self.sync - self.active
        if to_remove:
            can_reduce_quorum_by = self.quorum - 1
            # If we can reduce quorum size try to do so first
            if can_reduce_quorum_by:
                 # Pick nodes to remove by sorted order to provide deterministic behavior for tests
                remove = set(sorted(to_remove, reverse=True)[:can_reduce_quorum_by])
                yield self.sync_update(self.numsync, self.sync - remove)
                yield self.quorum_update(self.quorum - can_reduce_quorum_by, self.voters - remove)
                to_remove &= self.sync
            if to_remove:
                assert self.quorum == 1
                yield self.quorum_update(self.quorum, self.voters - to_remove)
                yield self.sync_update(self.numsync - len(to_remove), self.sync - to_remove)

        # If any new nodes, join them to quorum
        to_add = self.active - self.sync
        if to_add:
            # First get to requested replication factor
            increase_numsync_by = self.sync_wanted - self.numsync
            if increase_numsync_by:
                add = set(sorted(to_add)[:increase_numsync_by])
                yield self.sync_update(self.numsync + len(add), self.sync | add)
                yield self.quorum_update(self.quorum, self.voters | add)
                to_add -= self.sync
            if to_add:
                yield self.quorum_update(self.quorum + len(to_add), self.voters | to_add)
                yield self.sync_update(self.numsync, self.sync | to_add)

        # Apply requested replication factor change
        sync_increase = clamp(self.sync_wanted - self.numsync, min=2 - self.numsync, max=len(self.sync) - self.numsync)
        if sync_increase > 0:
            # Increase replication factor
            yield self.sync_update(self.numsync + sync_increase, self.sync)
            yield self.quorum_update(self.quorum - sync_increase, self.voters)
        elif sync_increase < 0:
            # Reduce replication factor
            yield self.quorum_update(self.quorum - sync_increase, self.voters)
            yield self.sync_update(self.numsync + sync_increase, self.sync)
