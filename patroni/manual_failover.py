from enum import Enum
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import datetime

    from .dcs import Cluster
    from .ha import Patroni
    from .utils import ParseScheduleErrors

from .utils import parse_schedule


class ManualFailoverPrecheckStatus(Enum):
    FAILOVER_NO_CANDIDATE = ('Failover could be performed only to a specific candidate', 400)
    SWITCHOVER_NO_LEADER = ('Switchover could be performed only from a specific leader', 400)
    SCHEDULED_FAILOVER = ("Failover can't be scheduled", 400)
    SCHEDULED_SWITCHOVER_PAUSE = ("Can't schedule switchover in the paused state", 400)
    SWITCHOVER_PAUSE_NO_CANDIDATE = ('Switchover is possible only to a specific candidate in a paused state', 400)
    SWITCHOVER_TO_LEADER = ('Switchover target and source are the same', 400)

    CLUSTER_NO_LEADER = ('Cluster {cluster_name} has no leader', 412)
    LEADER_NOT_MEMBER = ('Member {leader} is not the leader of cluster {cluster_name}', 412)
    CANDIDATE_NOT_SYNC_STANDBY = ('candidate name does not match with sync_standby', 412)
    NO_SYNC_CANDIDATE = ('{action} is not possible: can not find sync_standby', 412)
    ONLY_LEADER = ('{action} is not possible: cluster does not have members except leader', 412)
    CANDIDATE_NOT_MEMEBER = ('Member {candidate} does not exist in cluster {cluster_name} or is tagged as nofailover',
                             412)
    NO_GOOD_CANDIDATES = ('{action} is not possible: no good candidates have been found', 412)

    CHECK_PASSED = ('', None)


class ManualFailover(object):

    def __init__(self, action: str, cluster: 'Cluster',
                 leader: Optional[str], candidate: Optional[str], scheduled: Optional[str],
                 paused: bool = False, sync_mode: bool = False, patroni_obj: Optional['Patroni'] = None) -> None:
        self.action = action
        self.cluster = cluster
        self.leader = leader
        self.candidate = candidate
        self.scheduled = scheduled
        self.paused = paused
        self.sync_mode = sync_mode
        self.patroni = patroni_obj

    def parse_scheduled(self) -> Tuple[Optional['ParseScheduleErrors'], Optional['datetime.datetime']]:
        return parse_schedule(self.scheduled)

    def run_precheck(self) -> ManualFailoverPrecheckStatus:
        if self.action == 'failover' and not self.candidate:
            return ManualFailoverPrecheckStatus.FAILOVER_NO_CANDIDATE
        elif self.action == 'switchover' and not self.leader:
            return ManualFailoverPrecheckStatus.SWITCHOVER_NO_LEADER

        if self.scheduled:
            if self.action == 'failover':
                return ManualFailoverPrecheckStatus.SCHEDULED_FAILOVER
            elif self.paused:
                return ManualFailoverPrecheckStatus.SCHEDULED_SWITCHOVER_PAUSE

        if self.paused and not self.candidate:
            return ManualFailoverPrecheckStatus.SWITCHOVER_PAUSE_NO_CANDIDATE

        if self.leader == self.candidate:
            return ManualFailoverPrecheckStatus.SWITCHOVER_TO_LEADER

        if self.action == 'switchover':
            if self.cluster.leader is None or not self.cluster.leader.name:
                return ManualFailoverPrecheckStatus.CLUSTER_NO_LEADER
            if self.cluster.leader.name != self.leader:
                return ManualFailoverPrecheckStatus.LEADER_NOT_MEMBER

        if self.candidate:
            if self.action == 'switchover' and self.sync_mode and not self.cluster.sync.matches(self.candidate):
                return ManualFailoverPrecheckStatus.CANDIDATE_NOT_SYNC_STANDBY
            members = [m for m in self.cluster.members if m.name == self.candidate]
            if not members:
                return ManualFailoverPrecheckStatus.CANDIDATE_NOT_MEMEBER
        elif self.sync_mode:
            members = [m for m in self.cluster.members if self.cluster.sync.matches(m.name)]
            if not members:
                return ManualFailoverPrecheckStatus.NO_SYNC_CANDIDATE
        else:
            members = [m for m in self.cluster.members if not self.cluster.leader or m.name != self.cluster.leader.name and m.api_url]
            if not members:
                return ManualFailoverPrecheckStatus.ONLY_LEADER

        if self.patroni and not self.patroni.ha.has_members_eligible_to_promote(members, fast_path=True):
            return ManualFailoverPrecheckStatus.NO_GOOD_CANDIDATES

        return ManualFailoverPrecheckStatus.CHECK_PASSED
