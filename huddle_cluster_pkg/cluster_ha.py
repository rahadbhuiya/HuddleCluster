"""
HuddleCluster — Cluster High Availability (Raft-based)
=========================================================
Adds leader-election and state-replication to MasterNode so the cluster
coordinator has no single point of failure.

Architecture
------------
Each ``ClusterHA`` wraps a ``MasterNode`` and communicates with peer
``ClusterHA`` instances over HTTP.  The Raft protocol drives leader
election; the elected leader accepts all writes and periodically pushes
state snapshots to followers.  Followers serve read requests from their
local cache and redirect write requests to the leader (``HTTP 307``).

Raft roles
----------
``follower``  — initial state; waits for leader heartbeats.
``candidate`` — no heartbeat received within election timeout; starts
               a new election.
``leader``    — won a majority vote; sends periodic heartbeats and state
               snapshots to followers.

REST endpoints (mounted by MasterNode when ``ha=ClusterHA(...)`` is set):

    GET  /v1/ha/status         → role, term, leader, peers
    POST /v1/ha/vote           → RequestVote RPC (Raft)
    POST /v1/ha/sync           → AppendEntries / state snapshot from leader

Writes (join, heartbeat, leave, rollout/start …) on a follower return
``HTTP 307`` with ``X-Leader-URL`` header and a JSON body containing
``"leader_url"`` so clients can retry against the leader automatically.

Author : Rahad Bhuiya
Version: 3.4.0
License: MIT
"""

from __future__ import annotations

import json
import logging
import os
import random
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#  Raft constants 
FOLLOWER  = "follower"
CANDIDATE = "candidate"
LEADER    = "leader"

#  Partition & Split-brain states (v4.27.0) 
PARTITION_HEALTHY           = "healthy"
PARTITION_MAJORITY_ACTIVE   = "majority_active"
PARTITION_ISOLATED_MINORITY = "isolated_minority"
PARTITION_HEALING           = "healing"


class ClusterHA:
    """
    Simplified Raft HA layer with Split-Brain Detection & Auto-Healing (v4.27.0).
    Attach to a MasterNode::

        from huddle_cluster_pkg import MasterNode
        from huddle_cluster_pkg.cluster_ha import ClusterHA

        ha = ClusterHA(
            node_id="master-1",
            peers=["http://master-2:7071", "http://master-3:7072"],
        )
        master = MasterNode(port=7070, ha=ha)
        master.start()
    """

    def __init__(
        self,
        node_id: str,
        peers: Optional[List[str]] = None,
        election_timeout_sec: float = 2.0,
        heartbeat_interval_sec: float = 0.5,
        sync_interval_sec: float = 1.0,
        request_timeout_sec: float = 1.0,
        state_file: Optional[str] = None,
        quorum_timeout_sec: Optional[float] = None,
        auto_heal_enabled: bool = True,
    ) -> None:
        """
        Args:
            node_id:                Unique identifier for this HA node (used
                                    in vote RPCs and leader tracking).
            peers:                  Base URLs of the other HA-enabled masters,
                                    e.g. ``["http://host2:7071", "http://host3:7072"]``.
                                    Can be empty (standalone mode — always leader).
            election_timeout_sec:   Follower/candidate election timeout base.
                                    Actual timeout is randomised in
                                    [timeout, 2×timeout] to avoid split votes.
            heartbeat_interval_sec: How often the leader sends heartbeat RPCs.
            sync_interval_sec:      How often the leader pushes full state
                                    snapshots to followers.
            request_timeout_sec:    HTTP timeout for peer RPC calls.
            state_file:             Path to persist Raft term/voted_for
                                    across restarts (atomic write, same
                                    pattern as HuddleCluster.save_state()).
                                    Without this, term/voted_for are
                                    in-memory only — a restarted node can
                                    revote in a term it already voted in,
                                    which is a real (if narrow) Raft safety
                                    violation. Strongly recommended for any
                                    production HA deployment.
            quorum_timeout_sec:     Max elapsed time leader may run without a
                                    confirmed peer quorum before entering
                                    read-only split-brain guard. Defaults to
                                    1.5 * election_timeout_sec.
            auto_heal_enabled:      When True, nodes in partition/read-only mode
                                    automatically heal and resume when quorum or
                                    majority leader connectivity returns.
        """
        self._id               = node_id
        self._peers            = list(peers or [])
        self._base_timeout     = election_timeout_sec
        self._hb_interval      = heartbeat_interval_sec
        self._sync_interval    = sync_interval_sec
        self._rpc_timeout      = request_timeout_sec
        self._state_file       = state_file
        self._state_write_lock = threading.Lock()

        #  Raft persistent state 
        self._term             = 0
        self._voted_for: Optional[str] = None
        self._role             = FOLLOWER
        self._leader_id: Optional[str] = None
        self._leader_url: Optional[str] = None
        self._votes_received: set = set()

        #  Split-Brain & Partition State (v4.27.0) 
        self._quorum_timeout_sec = (
            quorum_timeout_sec
            if quorum_timeout_sec is not None
            else (self._base_timeout * 1.5)
        )
        self._auto_heal_enabled  = auto_heal_enabled
        self._readonly           = False
        self._readonly_reason: Optional[str] = None
        self._split_brain_detected = False
        self._last_quorum_time   = time.time()
        self._active_peers: set  = set()
        self._partition_state    = PARTITION_HEALTHY
        self._split_brain_count  = 0
        self._heal_count         = 0

        if self._state_file:
            self._load_persisted_state()

        #  Timing 
        self._last_hb          = time.time()
        self._election_timeout = self._new_timeout()

        #  Shared state 
        self._lock     = threading.RLock()
        self._running  = False
        self._master: Optional[Any] = None

        self._election_thread:  Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._sync_thread:      Optional[threading.Thread] = None

    
    # Lifecycle
    

    def attach(self, master: Any, self_url: str) -> None:
        """Called automatically by MasterNode.start().
        ``self_url`` is the full base URL of this master, e.g. ``http://host1:7070``."""
        self._master   = master
        self._self_url = self_url.rstrip("/")
        self._running  = True

        self._election_thread = threading.Thread(
            target=self._election_loop,
            name=f"ha-election-{self._id}",
            daemon=True,
        )
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"ha-heartbeat-{self._id}",
            daemon=True,
        )
        self._sync_thread = threading.Thread(
            target=self._sync_loop,
            name=f"ha-sync-{self._id}",
            daemon=True,
        )
        for t in (self._election_thread, self._heartbeat_thread, self._sync_thread):
            t.start()

        # Solo node — no peers — immediately becomes leader
        if not self._peers:
            self._become_leader()

        logger.info(
            "ClusterHA '%s' started (peers=%d, timeout=%.1fs)",
            self._id, len(self._peers), self._base_timeout,
        )

    def stop(self) -> None:
        self._running = False

    
    # Persistence — Raft term/voted_for survive a process restart
    

    def _load_persisted_state(self) -> None:
        try:
            with open(self._state_file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            self._term      = int(data.get("term", 0))
            self._voted_for = data.get("voted_for")
            logger.info(
                "HA '%s': restored persisted state from %s (term=%d, voted_for=%r)",
                self._id, self._state_file, self._term, self._voted_for,
            )
        except FileNotFoundError:
            pass   # first run — nothing to restore
        except (json.JSONDecodeError, ValueError, OSError) as e:
            logger.warning(
                "HA '%s': could not read state_file %s (%s) — "
                "starting from term 0 as if fresh. If this master previously "
                "voted, it may be eligible to vote again in an old term.",
                self._id, self._state_file, e,
            )

    def _persist(self) -> None:
        """Atomically write {term, voted_for} to state_file. Caller must
        hold self._lock (so the values read here are consistent), but the
        actual file write happens outside contention on _state_write_lock.
        No-op if state_file wasn't configured."""
        if not self._state_file:
            return
        payload = {"term": self._term, "voted_for": self._voted_for}
        with self._state_write_lock:
            tmp_path = None
            try:
                target_dir = os.path.dirname(os.path.abspath(self._state_file))
                os.makedirs(target_dir, exist_ok=True)
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".tmp", dir=target_dir,
                    delete=False, encoding="utf-8",
                ) as fh:
                    tmp_path = fh.name
                    json.dump(payload, fh)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, self._state_file)
                tmp_path = None
            except OSError as e:
                logger.error(
                    "HA '%s': failed to persist Raft state to %s: %s",
                    self._id, self._state_file, e,
                )
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

    
    # Public read-only state
    

    def is_leader(self) -> bool:
        with self._lock:
            # If in read-only mode or split-brain detected, refuse leader authority
            if self._readonly or self._split_brain_detected:
                return False
            return self._role == LEADER

    def is_ready(self) -> bool:
        """Readiness predicate for cloud probes and load balancers.
        Returns True if:
          - Node is LEADER (actively serving and coordinating)
          - Node is FOLLOWER and has identified an active leader
          - Standalone node (no peers)
        Returns False if:
          - CANDIDATE (election in progress) or uncoordinated FOLLOWER
          - Node is in read-only mode or isolated minority partition
        """
        with self._lock:
            if not self._running:
                return False
            if self._readonly or self._partition_state == PARTITION_ISOLATED_MINORITY:
                return False
            if self._role == LEADER:
                return True
            if self._role == FOLLOWER:
                return (not self._peers) or (self._leader_id is not None)
            return False

    def is_readonly(self) -> bool:
        """Return True if node is demoted to read-only due to partition or split-brain."""
        with self._lock:
            return self._readonly

    def is_split_brain(self) -> bool:
        """Return True if node has detected a cluster partition / split-brain hazard."""
        with self._lock:
            return self._split_brain_detected

    def partition_state(self) -> str:
        """Return current partition status ('healthy', 'majority_active', 'isolated_minority', 'healing')."""
        with self._lock:
            return self._partition_state

    def has_quorum(self) -> bool:
        """Return True if this node currently observes majority quorum."""
        with self._lock:
            if not self._peers:
                return True
            needed = (len(self._peers) + 1) // 2 + 1
            if self._role == LEADER:
                return (len(self._active_peers) + 1) >= needed
            # For follower: has active leader within timeout and not marked isolated
            recent_hb = (time.time() - self._last_hb) <= (self._base_timeout * 2)
            return (self._leader_id is not None) and recent_hb and (not self._readonly)

    def detect_split_brain(self) -> Dict[str, Any]:
        """
        Comprehensive diagnostic inspection of cluster partition and quorum health.
        """
        with self._lock:
            needed = (len(self._peers) + 1) // 2 + 1
            has_q = self.has_quorum()
            return {
                "is_split_brain":       self._split_brain_detected,
                "is_readonly":          self._readonly,
                "readonly_reason":      self._readonly_reason,
                "partition_state":      self._partition_state,
                "has_quorum":           has_q,
                "role":                 self._role,
                "reachable_peers":      sorted(self._active_peers),
                "reachable_peer_count": len(self._active_peers),
                "required_quorum":      needed,
                "total_cluster_size":   len(self._peers) + 1,
                "split_brain_count":    self._split_brain_count,
                "heal_count":           self._heal_count,
            }

    def enter_readonly_mode(self, reason: str = "manual") -> None:
        """
        Place node into safe read-only mode to prevent stale or conflicting writes.
        If current role is LEADER, steps down to FOLLOWER.
        """
        with self._lock:
            self._readonly = True
            self._readonly_reason = reason
            self._split_brain_detected = True
            self._partition_state = PARTITION_ISOLATED_MINORITY
            if self._role == LEADER:
                logger.warning(
                    "HA '%s': entering read-only mode (%s) — stepping down from leader",
                    self._id, reason,
                )
                self._role = FOLLOWER
                self._leader_id = None
                self._leader_url = None
                self._reset_timeout()
            else:
                logger.warning(
                    "HA '%s': entering read-only mode (%s)", self._id, reason
                )

    def exit_readonly_mode(self) -> None:
        """Manually or programmatically lift read-only mode."""
        with self._lock:
            self._readonly = False
            self._readonly_reason = None
            self._split_brain_detected = False
            self._partition_state = PARTITION_HEALTHY
            logger.info("HA '%s': exited read-only mode", self._id)

    def auto_heal(self) -> bool:
        """
        Attempt auto-healing by checking if quorum or legitimate leader has returned.
        Returns True if healed, False otherwise.
        """
        with self._lock:
            if not self._auto_heal_enabled:
                return False
            if not (self._readonly or self._split_brain_detected):
                return True
            # If standalone or connected to a legitimate leader
            if not self._peers or (self._leader_id is not None and (time.time() - self._last_hb <= self._base_timeout * 2)):
                self._trigger_auto_heal("leader_heartbeat_verified")
                return True
            return False

    def _trigger_auto_heal(self, reason: str, leader_id: Optional[str] = None) -> None:
        """Caller must hold self._lock (or call from single-threaded lock context)."""
        self._readonly = False
        self._readonly_reason = None
        self._split_brain_detected = False
        self._partition_state = PARTITION_HEALTHY
        self._heal_count += 1
        logger.info(
            "HA '%s': AUTO-HEAL SUCCEEDED (%s, leader=%s). Normal cluster operations restored.",
            self._id, reason, leader_id or self._leader_id,
        )

    def role(self) -> str:
        with self._lock:
            return self._role

    def leader_url(self) -> Optional[str]:
        with self._lock:
            return self._leader_url

    def status(self) -> Dict[str, Any]:
        with self._lock:
            needed = (len(self._peers) + 1) // 2 + 1
            has_q = self.has_quorum()
            return {
                "node_id":              self._id,
                "role":                 self._role,
                "term":                 self._term,
                "leader_id":            self._leader_id,
                "leader_url":           self._leader_url,
                "peers":                self._peers,
                "peer_count":           len(self._peers),
                "self_url":             getattr(self, "_self_url", None),
                "readonly":             self._readonly,
                "readonly_reason":      self._readonly_reason,
                "split_brain":          self._split_brain_detected,
                "partition_state":      self._partition_state,
                "has_quorum":           has_q,
                "reachable_peer_count": len(self._active_peers),
                "required_quorum":      needed,
                "split_brain_count":    self._split_brain_count,
                "heal_count":           self._heal_count,
            }

    
    # Raft RPCs (called via HTTP endpoints on the master)
    

    def handle_vote_request(
        self, candidate_id: str, candidate_term: int
    ) -> Dict[str, Any]:
        """RequestVote RPC handler."""
        with self._lock:
            if candidate_term < self._term:
                return {"vote_granted": False, "term": self._term}

            if candidate_term > self._term:
                self._step_down(candidate_term)

            # Grant vote if we haven't voted yet (or voted for this candidate)
            can_vote = (
                self._voted_for is None
                or self._voted_for == candidate_id
            )
            if can_vote:
                self._voted_for = candidate_id
                self._persist()
                self._reset_timeout()
                logger.info(
                    "HA '%s': voted for '%s' in term %d",
                    self._id, candidate_id, self._term,
                )
                return {"vote_granted": True, "term": self._term}

            return {"vote_granted": False, "term": self._term}

    def handle_append_entries(
        self, leader_id: str, leader_url: str, term: int,
        state_snapshot: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """AppendEntries RPC handler (heartbeat + optional state sync)."""
        with self._lock:
            if term < self._term:
                return {"success": False, "term": self._term}

            if term > self._term:
                self._step_down(term)

            self._leader_id  = leader_id
            self._leader_url = leader_url
            self._role       = FOLLOWER
            self._reset_timeout()

            # Auto-heal from partition/read-only mode when reconnected to legitimate leader
            if self._auto_heal_enabled and (self._readonly or self._split_brain_detected):
                self._trigger_auto_heal("reconnected_to_leader", leader_id)

        if state_snapshot is not None:
            self._apply_snapshot(state_snapshot)

        return {"success": True, "term": term}

    
    # Snapshot application (state replication from leader)
    

    def _apply_snapshot(self, snapshot: Dict) -> None:
        """Overwrite local node registry with the leader's snapshot."""
        if self._master is None:
            return
        try:
            from huddle_cluster_pkg.cluster_master import NodeRecord
            import dataclasses
            new_nodes = {}
            for node_id, nd in snapshot.items():
                rec = NodeRecord(
                    node_id         = nd["node_id"],
                    address         = nd["address"],
                    port            = nd["port"],
                    metadata        = nd.get("metadata", {}),
                    status          = nd.get("status", "alive"),
                    heartbeat_count = nd.get("heartbeat_count", 0),
                    death_count     = nd.get("death_count", 0),
                )
                # Preserve timing fields if available
                if "last_heartbeat" in nd:
                    rec.last_heartbeat = nd["last_heartbeat"]
                if "joined_at" in nd:
                    rec.joined_at = nd["joined_at"]
                new_nodes[node_id] = rec

            with self._master._lock:
                self._master._nodes = new_nodes
            logger.debug(
                "HA '%s': applied snapshot (%d nodes)", self._id, len(new_nodes)
            )
        except Exception:
            logger.exception("HA '%s': failed to apply snapshot", self._id)

    def _build_snapshot(self) -> Dict:
        """Serialise the local node registry for replication."""
        if self._master is None:
            return {}
        with self._master._lock:
            return {
                nid: nd.to_dict()
                for nid, nd in self._master._nodes.items()
            }

    
    # Election loop
    

    def _election_loop(self) -> None:
        while self._running:
            time.sleep(0.05)
            with self._lock:
                role    = self._role
                elapsed = time.time() - self._last_hb
                timeout = self._election_timeout

            if role == FOLLOWER and elapsed > timeout:
                self._start_election()
            elif role == CANDIDATE and elapsed > timeout:
                # Split vote / no quorum — try again
                self._start_election()

    def _start_election(self) -> None:
        with self._lock:
            self._term        += 1
            self._role         = CANDIDATE
            self._voted_for    = self._id
            self._persist()
            self._votes_received = {self._id}   # vote for self
            self._leader_id   = None
            self._leader_url  = None
            self._reset_timeout()
            term       = self._term
            candidates = list(self._peers)

        logger.info(
            "HA '%s': starting election for term %d (%d peer(s))",
            self._id, term, len(candidates),
        )

        votes   = 1   # self-vote
        needed  = (len(self._peers) + 1) // 2 + 1   # majority of cluster

        for peer_url in candidates:
            try:
                resp = self._rpc_post(
                    peer_url + "/v1/ha/vote",
                    {"candidate_id": self._id, "candidate_term": term},
                )
                if resp.get("term", 0) > term:
                    with self._lock:
                        self._step_down(resp["term"])
                    return
                if resp.get("vote_granted"):
                    votes += 1
            except Exception:
                pass   # peer unreachable — treat as no-vote

        if votes >= needed:
            self._become_leader()
        else:
            logger.info(
                "HA '%s': election failed (%d/%d votes)", self._id, votes, needed
            )

    def _become_leader(self) -> None:
        with self._lock:
            self._role         = LEADER
            self._leader_id    = self._id
            self._leader_url   = getattr(self, "_self_url", None)
            self._reset_timeout()
            term = self._term
        logger.info(
            "HA '%s': became LEADER for term %d", self._id, self._term
        )
        # Push state immediately rather than waiting for the next
        # _sync_loop tick — otherwise a freshly-elected leader that
        # crashes within sync_interval_sec could hand leadership to a
        # follower still holding stale state (a real, if narrow,
        # leader-completeness gap in this snapshot-replication model).
        snapshot = self._build_snapshot()
        for peer_url in self._peers:
            try:
                self._rpc_post(
                    peer_url + "/v1/ha/sync",
                    {
                        "leader_id":      self._id,
                        "leader_url":     getattr(self, "_self_url", ""),
                        "term":           term,
                        "state_snapshot": snapshot,
                    },
                )
            except Exception:
                pass   # peer unreachable — regular _sync_loop will retry


    # Heartbeat loop (leader only)
    

    def _heartbeat_loop(self) -> None:
        while self._running:
            time.sleep(self._hb_interval)
            if not self._running:
                break
            with self._lock:
                if self._role != LEADER:
                    continue
                term = self._term

            active_in_round: set = set()
            stepped_down = False
            for peer_url in self._peers:
                try:
                    resp = self._rpc_post(
                        peer_url + "/v1/ha/sync",
                        {
                            "leader_id":  self._id,
                            "leader_url": getattr(self, "_self_url", ""),
                            "term":       term,
                        },
                    )
                    if resp.get("term", 0) > term:
                        with self._lock:
                            self._step_down(resp["term"])
                        stepped_down = True
                        break
                    if resp.get("success"):
                        active_in_round.add(peer_url)
                except Exception:
                    pass   # peer unreachable

            if stepped_down:
                continue

            # Quorum & Split-Brain Evaluation (v4.27.0)
            with self._lock:
                if self._role != LEADER:
                    continue
                self._active_peers = active_in_round
                needed = (len(self._peers) + 1) // 2 + 1
                total_reachable = len(active_in_round) + 1  # includes self

                if total_reachable >= needed:
                    self._last_quorum_time = time.time()
                    if self._split_brain_detected or self._readonly:
                        self._trigger_auto_heal("quorum_restored")
                    else:
                        self._partition_state = (
                            PARTITION_HEALTHY
                            if len(active_in_round) == len(self._peers)
                            else PARTITION_MAJORITY_ACTIVE
                        )
                else:
                    # Minority partition! Check quorum timeout
                    elapsed = time.time() - self._last_quorum_time
                    if elapsed >= self._quorum_timeout_sec:
                        self._split_brain_count += 1
                        self.enter_readonly_mode(
                            f"quorum_lost_{total_reachable}_of_{needed}_nodes_reachable"
                        )

    
    # State-sync loop (leader only)
    

    def _sync_loop(self) -> None:
        while self._running:
            time.sleep(self._sync_interval)
            if not self._running:
                break
            with self._lock:
                if self._role != LEADER:
                    continue
                term = self._term

            snapshot = self._build_snapshot()
            for peer_url in self._peers:
                try:
                    self._rpc_post(
                        peer_url + "/v1/ha/sync",
                        {
                            "leader_id":      self._id,
                            "leader_url":     getattr(self, "_self_url", ""),
                            "term":           term,
                            "state_snapshot": snapshot,
                        },
                    )
                except Exception:
                    pass   # peer unreachable — they'll catch up later

    
    # Internal helpers
    

    def _step_down(self, new_term: int) -> None:
        """Revert to follower with a higher term.  Caller must hold lock."""
        self._term       = new_term
        self._role       = FOLLOWER
        self._voted_for  = None
        self._persist()
        self._leader_id  = None
        self._leader_url = None
        self._reset_timeout()
        logger.info(
            "HA '%s': stepped down to follower (new term %d)",
            self._id, new_term,
        )

    def _reset_timeout(self) -> None:
        """Caller must hold lock (or call from single-threaded context)."""
        self._last_hb          = time.time()
        self._election_timeout = self._new_timeout()

    def _new_timeout(self) -> float:
        """Randomised election timeout in [base, 2×base]."""
        return self._base_timeout * (1.0 + random.random())

    def _rpc_post(self, url: str, payload: Dict) -> Dict:
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=self._rpc_timeout) as r:
            return json.loads(r.read())