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
import random
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


class ClusterHA:
    """
    Simplified Raft HA layer.  Attach to a MasterNode::

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
        """
        self._id               = node_id
        self._peers            = list(peers or [])
        self._base_timeout     = election_timeout_sec
        self._hb_interval      = heartbeat_interval_sec
        self._sync_interval    = sync_interval_sec
        self._rpc_timeout      = request_timeout_sec

        #  Raft persistent state (simplified — in-memory) 
        self._term             = 0
        self._voted_for: Optional[str] = None
        self._role             = FOLLOWER
        self._leader_id: Optional[str] = None
        self._leader_url: Optional[str] = None
        self._votes_received: set = set()

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

    
    # Public read-only state
    

    def is_leader(self) -> bool:
        with self._lock:
            return self._role == LEADER

    def role(self) -> str:
        with self._lock:
            return self._role

    def leader_url(self) -> Optional[str]:
        with self._lock:
            return self._leader_url

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id":    self._id,
                "role":       self._role,
                "term":       self._term,
                "leader_id":  self._leader_id,
                "leader_url": self._leader_url,
                "peers":      self._peers,
                "peer_count": len(self._peers),
                "self_url":   getattr(self, "_self_url", None),
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
        logger.info(
            "HA '%s': became LEADER for term %d", self._id, self._term
        )


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
                        break
                except Exception:
                    pass   # peer unreachable

    
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