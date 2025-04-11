"""Implement state machine to manage ``synchronous_standby_names`` GUC and ``/sync`` key in DCS."""
import logging

from typing import Collection, Iterator, NamedTuple, Optional

from .collections import CaseInsensitiveSet
from .exceptions import PatroniException

logger = logging.getLogger(__name__)


class Transition(NamedTuple):
    """Object describing transition of ``/sync`` or ``synchronous_standby_names`` to the new state.

    .. note::
        Object attributes represent the new state.

    :ivar transition_type: possible values:

        * ``sync`` - indicates that we needed to update ``synchronous_standby_names``.
        * ``quorum`` - indicates that we need to update ``/sync`` key in DCS.
        * ``restart`` - caller should stop iterating over transitions and restart :class:`QuorumStateResolver`.
    :ivar leader: the new value of the ``leader`` field in the ``/sync`` key.
    :ivar num: the new value of the synchronous nodes count in ``synchronous_standby_names`` or value of the ``quorum``
               field in the ``/sync`` key for :attr:`transition_type` values ``sync`` and ``quorum`` respectively.
    :ivar names: the new value of node names listed in ``synchronous_standby_names`` or value of ``voters``
                 field in the ``/sync`` key  for :attr:`transition_type` values ``sync`` and ``quorum`` respectively.
    """

    transition_type: str
    leader: str
    num: int
    names: CaseInsensitiveSet


class QuorumError(PatroniException):
    """Exception indicating that the quorum state is broken."""


class QuorumStateResolver:
    """Calculates a list of state transitions and yields them as :class:`Transition` named tuples.

    Synchronous replication state is set in two places:

    * PostgreSQL configuration sets how many and which nodes are needed for a commit to succeed, abbreviated as
      ``numsync`` and ``sync`` set here;
    * DCS contains information about how many and which nodes need to be interrogated to be sure to see an wal position
      containing latest confirmed commit, abbreviated as ``quorum`` and ``voters`` set.

    .. note::
        Both of above pairs have the meaning "ANY n OF set".

        The number of nodes needed for commit to succeed, ``numsync``, is also called the replication factor.

    To guarantee zero transaction loss on failover we need to keep the invariant that at all times any subset of
    nodes that can acknowledge a commit overlaps with any subset of nodes that can achieve quorum to promote a new
    leader. Given a desired replication factor and a set of nodes able to participate in sync replication there
    is one optimal state satisfying this condition. Given the node set ``active``, the optimal state is::

        sync = voters = active

        numsync = min(sync_wanted, len(active))

        quorum = len(active) - numsync

    We need to be able to produce a series of state changes that take the system to this desired state from any
    other arbitrary state given arbitrary changes is node availability, configuration and interrupted transitions.

    To keep the invariant the rule to follow is that when increasing ``numsync`` or ``quorum``, we need to perform the
    increasing operation first. When decreasing either, the decreasing operation needs to be performed later. In other
    words:

    * If a user increases ``synchronous_node_count`` configuration, first we increase ``synchronous_standby_names``
      (``numsync``), then we decrease ``quorum`` field in the ``/sync`` key;
    * If a user decreases ``synchronous_node_count`` configuration, first we increase ``quorum`` field in the ``/sync``
      key, then we decrease ``synchronous_standby_names`` (``numsync``).

    Order of adding or removing nodes from ``sync`` and ``voters`` depends on the state of
    ``synchronous_standby_names``.

    When adding new nodes::

        if ``sync`` (``synchronous_standby_names``) is empty:
            add new nodes first to ``sync`` and then to ``voters`` when ``numsync_confirmed`` > ``0``.
        else:
            add new nodes first to ``voters`` and then to ``sync``.

    When removing nodes::

        if ``sync`` (``synchronous_standby_names``) will become empty after removal:
            first remove nodes from ``voters`` and then from ``sync``.
        else:
            first remove nodes from ``sync`` and then from ``voters``.
            Make ``voters`` empty if ``numsync_confirmed`` == ``0``.

    :ivar leader: name of the leader, according to the ``/sync`` key.
    :ivar quorum: ``quorum`` value from the ``/sync`` key, the minimal number of nodes we need see
                  when doing the leader race.
    :ivar voters: ``sync_standby`` value from the ``/sync`` key, set of node names we will be
                  running the leader race against.
    :ivar numsync: the number of synchronous nodes from the ``synchronous_standby_names``.
    :ivar sync: set of node names listed in the ``synchronous_standby_names``.
    :ivar numsync_confirmed: the number of nodes that are confirmed to reach "safe" LSN after they were added to the
                  ``synchronous_standby_names``.
    :ivar active: set of node names that are replicating from the primary (according to ``pg_stat_replication``)
                  and are eligible to be listed in ``synchronous_standby_names``.
    :ivar sync_wanted: desired number of synchronous nodes (``synchronous_node_count`` from the global configuration).
    :ivar leader_wanted: the desired leader (could be different from the :attr:`leader` right after a failover).
    """

    def __init__(self, leader: str, quorum: int, voters: Collection[str],
                 numsync: int, sync: Collection[str], numsync_confirmed: int,
                 active: Collection[str], sync_wanted: int, leader_wanted: str) -> None:
        """Instantiate :class:``QuorumStateResolver`` based on input parameters.

        :param leader: name of the leader, according to the ``/sync`` key.
        :param quorum: ``quorum`` value from the ``/sync`` key, the minimal number of nodes we need see
                        when doing the leader race.
        :param voters: ``sync_standby`` value from the ``/sync`` key, set of node names we will be
                       running the leader race against.
        :param numsync: the number of synchronous nodes from the ``synchronous_standby_names``.
        :param sync: Set of node names listed in the ``synchronous_standby_names``.
        :param numsync_confirmed: the number of nodes that are confirmed to reach "safe" LSN after
                                  they were added to the ``synchronous_standby_names``.
        :param active: set of node names that are replicating from the primary (according to ``pg_stat_replication``)
                       and are eligible to be listed in ``synchronous_standby_names``.
        :param sync_wanted: desired number of synchronous nodes
                            (``synchronous_node_count`` from the global configuration).
        :param leader_wanted: the desired leader (could be different from the *leader* right after a failover).

        """
        self.leader = leader
        self.quorum = quorum
        self.voters = CaseInsensitiveSet(voters)
        self.numsync = min(numsync, len(sync))  # numsync can't be bigger than number of listed synchronous nodes.
        self.sync = CaseInsensitiveSet(sync)
        self.numsync_confirmed = numsync_confirmed
        self.active = CaseInsensitiveSet(active)
        self.sync_wanted = sync_wanted
        self.leader_wanted = leader_wanted

    def check_invariants(self) -> None:
        """Checks invariant of ``synchronous_standby_names`` and ``/sync`` key in DCS.

        .. seealso::
            Check :class:`QuorumStateResolver`'s docstring for more information.

        :raises:
            :exc:`QuorumError`: in case of broken state"""
        voters = CaseInsensitiveSet(self.voters | CaseInsensitiveSet([self.leader]))
        sync = CaseInsensitiveSet(self.sync | CaseInsensitiveSet([self.leader_wanted]))

        # We need to verify that subset of nodes that can acknowledge a commit overlaps
        # with any subset of nodes that can achieve quorum to promote a new leader.
        # ``+ 1`` is required because the leader is included in the set.
        if self.voters and not (len(voters | sync) <= self.quorum + self.numsync + 1):
            len_nodes = len(voters | sync)
            raise QuorumError("Quorum and sync not guaranteed to overlap: "
                              f"nodes {len_nodes} >= quorum {self.quorum} + sync {self.sync} + 1")
        # unstable cases, we are changing synchronous_standby_names and /sync key
        # one after another, hence one set is allowed to be a subset of another
        if not (voters.issubset(sync) or sync.issubset(voters)):
            voters_only = voters - sync
            sync_only = sync - voters
            raise QuorumError(f"Mismatched sets: voter only={voters_only} sync only={sync_only}")

    def quorum_update(self, quorum: int, voters: CaseInsensitiveSet, leader: Optional[str] = None,
                      adjust_quorum: Optional[bool] = True) -> Iterator[Transition]:
        """Updates :attr:`quorum`, :attr:`voters` and optionally :attr:`leader` fields.

        :param quorum: the new value for :attr:`quorum`, could be adjusted depending
                       on values of :attr:`numsync_confirmed` and *adjust_quorum*.
        :param voters: the new value for :attr:`voters`, could be adjusted if :attr:`numsync_confirmed` == ``0``.
        :param leader: the new value for :attr:`leader`, optional.
        :param adjust_quorum: if set to ``True`` the quorum requirement will be increased by the
                              difference between :attr:`numsync` and :attr:`numsync_confirmed`.

        :yields: the new state of the ``/sync`` key as a :class:`Transition` object.

        :raises:
            :exc:`QuorumError` in case of invalid data or if the invariant after transition could not be satisfied.
        """
        if quorum < 0:
            raise QuorumError(f'Quorum {quorum} < 0 of ({voters})')
        if quorum > 0 and quorum >= len(voters):
            raise QuorumError(f'Quorum {quorum} >= N of ({voters})')

        old_leader = self.leader
        if leader is not None:  # Change of leader was requested
            self.leader = leader
        elif self.numsync_confirmed == 0 and not self.voters:
            # If there are no nodes that known to caught up with the primary we want to reset quorum/voters in /sync key
            quorum = 0
            voters = CaseInsensitiveSet()
        elif adjust_quorum:
            # It could be that the number of nodes that are known to catch up with the primary is below desired numsync.
            # We want to increase quorum to guarantee that the sync node will be found during the leader race.
            quorum += max(self.numsync - self.numsync_confirmed, 0)

        if (self.leader, quorum, voters) == (old_leader, self.quorum, self.voters):
            if self.voters:
                return
            # If transition produces no change of leader/quorum/voters we want to give a hint to
            # the caller to fetch the new state from the database and restart QuorumStateResolver.
            yield Transition('restart', self.leader, self.quorum, self.voters)

        self.quorum = quorum
        self.voters = voters
        self.check_invariants()
        logger.debug('quorum %s %s %s', self.leader, self.quorum, self.voters)
        yield Transition('quorum', self.leader, self.quorum, self.voters)

    def sync_update(self, numsync: int, sync: CaseInsensitiveSet) -> Iterator[Transition]:
        """Updates :attr:`numsync` and :attr:`sync` fields.

        :param numsync: the new value for :attr:`numsync`.
        :param sync: the new value for :attr:`sync`:

        :yields: the new state of ``synchronous_standby_names`` as a :class:`Transition` object.

        :raises:
            :exc:`QuorumError` in case of invalid data or if invariant after transition could not be satisfied
        """
        if numsync < 0:
            raise QuorumError(f'Sync {numsync} < 0 of ({sync})')
        if numsync > len(sync):
            raise QuorumError(f'Sync {numsync} > N of ({sync})')

        self.numsync = numsync
        self.sync = sync
        self.check_invariants()
        logger.debug('sync %s %s %s', self.leader, self.numsync, self.sync)
        yield Transition('sync', self.leader, self.numsync, self.sync)

    def __iter__(self) -> Iterator[Transition]:
        """Iterate over the transitions produced by :meth:`_generate_transitions`.

        .. note::
            Merge two transitions of the same type to a single one.

            This is always safe because skipping the first transition is equivalent
            to no one observing the intermediate state.

        :yields: transitions as :class:`Transition` objects.
        """
        transitions = list(self._generate_transitions())
        for cur_transition, next_transition in zip(transitions, transitions[1:] + [None]):
            if isinstance(next_transition, Transition) \
                    and cur_transition.transition_type == next_transition.transition_type:
                continue
            yield cur_transition
            if cur_transition.transition_type == 'restart':
                break

    def __handle_non_steady_cases(self) -> Iterator[Transition]:
        """Handle cases when set of transitions produced on previous run was interrupted.

        :yields: transitions as :class:`Transition` objects.
        """
        if self.sync < self.voters:
            logger.debug("Case 1: synchronous_standby_names %s is a subset of DCS state %s", self.sync, self.voters)
            # Case 1: voters is superset of sync nodes. In the middle of changing voters (quorum).
            # Evict  dead nodes from voters that are not being synced.
            remove_from_voters = self.voters - (self.sync | self.active)
            if remove_from_voters:
                yield from self.quorum_update(
                    quorum=len(self.voters) - len(remove_from_voters) - self.numsync,
                    voters=CaseInsensitiveSet(self.voters - remove_from_voters),
                    adjust_quorum=not (self.sync - self.active))
            # Start syncing to nodes that are in voters and alive
            add_to_sync = (self.voters & self.active) - self.sync
            if add_to_sync:
                yield from self.sync_update(self.numsync, CaseInsensitiveSet(self.sync | add_to_sync))
        elif self.sync > self.voters:
            logger.debug("Case 2: synchronous_standby_names %s is a superset of DCS state %s", self.sync, self.voters)
            # Case 2: sync is superset of voters nodes. In the middle of changing replication factor (sync).
            # Add to voters nodes that are already synced and active
            remove_from_sync = self.sync - self.active
            sync = CaseInsensitiveSet(self.sync - remove_from_sync)
            # If sync will not become empty after removing dead nodes - remove them.
            # However, do it carefully, between sync and voters should remain common nodes!
            if remove_from_sync and sync and (not self.voters or sync & self.voters):
                yield from self.sync_update(min(self.numsync, len(self.sync) - len(remove_from_sync)), sync)
            add_to_voters = (self.sync - self.voters) & self.active
            if add_to_voters:
                voters = CaseInsensitiveSet(self.voters | add_to_voters)
                yield from self.quorum_update(len(voters) - self.numsync, voters)
            # Remove from sync nodes that are dead
            remove_from_sync = self.sync - self.voters
            if remove_from_sync:
                yield from self.sync_update(
                    numsync=min(self.numsync, len(self.sync) - len(remove_from_sync)),
                    sync=CaseInsensitiveSet(self.sync - remove_from_sync))

        # After handling these two cases voters and sync must match.
        assert self.voters == self.sync

        safety_margin = self.quorum + min(self.numsync, self.numsync_confirmed) - len(self.voters | self.sync)
        if safety_margin > 0:  # In the middle of changing replication factor.
            if self.numsync > self.sync_wanted:
                numsync = max(self.sync_wanted, len(self.voters) - self.quorum)
                logger.debug('Case 3: replication factor %d is bigger than needed %d', self.numsync, numsync)
                yield from self.sync_update(numsync, self.sync)
            else:
                quorum = len(self.sync) - self.numsync
                logger.debug('Case 4: quorum %d is bigger than needed %d', self.quorum, quorum)
                yield from self.quorum_update(quorum, self.voters)
        else:
            safety_margin = self.quorum + self.numsync - len(self.voters | self.sync)
            if self.numsync == self.sync_wanted and safety_margin > 0 and self.numsync > self.numsync_confirmed:
                yield from self.quorum_update(len(self.sync) - self.numsync, self.voters)

    def __remove_gone_nodes(self) -> Iterator[Transition]:
        """Remove inactive nodes from ``synchronous_standby_names`` and from ``/sync`` key.

        :yields: transitions as :class:`Transition` objects.
        """
        to_remove = self.sync - self.active
        if to_remove and self.sync == to_remove:
            logger.debug("Removing nodes: %s", to_remove)
            yield from self.quorum_update(0, CaseInsensitiveSet(), adjust_quorum=False)
            yield from self.sync_update(0, CaseInsensitiveSet())
        elif to_remove:
            logger.debug("Removing nodes: %s", to_remove)
            can_reduce_quorum_by = self.quorum
            # If we can reduce quorum size try to do so first
            if can_reduce_quorum_by:
                # Pick nodes to remove by sorted order to provide deterministic behavior for tests
                remove = CaseInsensitiveSet(sorted(to_remove, reverse=True)[:can_reduce_quorum_by])
                sync = CaseInsensitiveSet(self.sync - remove)
                # when removing nodes from sync we can safely increase numsync if requested
                numsync = min(self.sync_wanted if self.sync_wanted > self.numsync else self.numsync, len(sync))
                yield from self.sync_update(numsync, sync)
                voters = CaseInsensitiveSet(self.voters - remove)
                to_remove &= self.sync
                yield from self.quorum_update(len(voters) - self.numsync, voters,
                                              adjust_quorum=not to_remove)
            if to_remove:
                assert self.quorum == 0
                numsync = self.numsync - len(to_remove)
                sync = CaseInsensitiveSet(self.sync - to_remove)
                voters = CaseInsensitiveSet(self.voters - to_remove)
                sync_decrease = numsync - min(self.sync_wanted, len(sync))
                quorum = min(sync_decrease, len(voters) - 1) if sync_decrease else 0
                yield from self.quorum_update(quorum, voters, adjust_quorum=False)
                yield from self.sync_update(numsync, sync)

    def __add_new_nodes(self) -> Iterator[Transition]:
        """Add new active nodes to ``synchronous_standby_names`` and to ``/sync`` key.

        :yields: transitions as :class:`Transition` objects.
        """
        to_add = self.active - self.sync
        if to_add:
            # First get to requested replication factor
            logger.debug("Adding nodes: %s", to_add)
            sync_wanted = min(self.sync_wanted, len(self.sync | to_add))
            increase_numsync_by = sync_wanted - self.numsync
            if increase_numsync_by > 0:
                if self.sync:
                    add = CaseInsensitiveSet(sorted(to_add)[:increase_numsync_by])
                    increase_numsync_by = len(add)
                else:  # there is only the leader
                    add = to_add  # and it is safe to add all nodes at once if sync is empty
                yield from self.sync_update(self.numsync + increase_numsync_by, CaseInsensitiveSet(self.sync | add))
                voters = CaseInsensitiveSet(self.voters | add)
                yield from self.quorum_update(len(voters) - sync_wanted, voters)
                to_add -= self.sync
            if to_add:
                voters = CaseInsensitiveSet(self.voters | to_add)
                yield from self.quorum_update(len(voters) - sync_wanted, voters,
                                              adjust_quorum=sync_wanted > self.numsync_confirmed)
                yield from self.sync_update(sync_wanted, CaseInsensitiveSet(self.sync | to_add))

    def __handle_replication_factor_change(self) -> Iterator[Transition]:
        """Handle change of the replication factor (:attr:`sync_wanted`, aka ``synchronous_node_count``).

        :yields: transitions as :class:`Transition` objects.
        """
        # Apply requested replication factor change
        sync_increase = min(self.sync_wanted, len(self.sync)) - self.numsync
        if sync_increase > 0:
            # Increase replication factor
            logger.debug("Increasing replication factor to %s", self.numsync + sync_increase)
            yield from self.sync_update(self.numsync + sync_increase, self.sync)
            yield from self.quorum_update(len(self.voters) - self.numsync, self.voters)
        elif sync_increase < 0:
            # Reduce replication factor
            logger.debug("Reducing replication factor to %s", self.numsync + sync_increase)
            if self.quorum - sync_increase < len(self.voters):
                yield from self.quorum_update(len(self.voters) - self.numsync - sync_increase, self.voters,
                                              adjust_quorum=self.sync_wanted > self.numsync_confirmed)
            yield from self.sync_update(self.numsync + sync_increase, self.sync)

    def _generate_transitions(self) -> Iterator[Transition]:
        """Produce a set of changes to safely transition from the current state to the desired.

        :yields: transitions as :class:`Transition` objects.
        """
        logger.debug("Quorum state: leader %s quorum %s, voters %s, numsync %s, sync %s, "
                     "numsync_confirmed %s, active %s, sync_wanted %s leader_wanted %s",
                     self.leader, self.quorum, self.voters, self.numsync, self.sync,
                     self.numsync_confirmed, self.active, self.sync_wanted, self.leader_wanted)
        try:
            if self.leader_wanted != self.leader:  # failover
                voters = (self.voters - CaseInsensitiveSet([self.leader_wanted])) | CaseInsensitiveSet([self.leader])
                if not self.sync:
                    # If sync is empty we need to update synchronous_standby_names first
                    numsync = len(voters) - self.quorum
                    yield from self.sync_update(numsync, CaseInsensitiveSet(voters))
                # If leader changed we need to add the old leader to quorum (voters)
                yield from self.quorum_update(self.quorum, CaseInsensitiveSet(voters), self.leader_wanted)
                # right after promote there could be no replication connections yet
                if not self.sync & self.active:
                    return  # give another loop_wait seconds for replicas to reconnect before removing them from quorum
            else:
                self.check_invariants()
        except QuorumError as e:
            logger.warning('%s', e)
            yield from self.quorum_update(len(self.sync) - self.numsync, self.sync)

        assert self.leader == self.leader_wanted

        # numsync_confirmed could be 0 after restart/failover, we will calculate it from quorum
        if self.numsync_confirmed == 0 and self.sync & self.active:
            self.numsync_confirmed = min(len(self.sync & self.active), len(self.voters) - self.quorum)
            logger.debug('numsync_confirmed=0, adjusting it to %d', self.numsync_confirmed)

        yield from self.__handle_non_steady_cases()

        # We are in a steady state point. Find if desired state is different and act accordingly.

        yield from self.__remove_gone_nodes()

        yield from self.__add_new_nodes()

        yield from self.__handle_replication_factor_change()
