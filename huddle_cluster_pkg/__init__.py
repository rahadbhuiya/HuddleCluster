"""
huddle_cluster_pkg — Extension package for HuddleCluster.
==========================================================

Cluster management (v2.0.0)
----------------------------
    MasterNode      Central coordinator: node registry, heartbeat tracking,
                    REST API for CLI and dashboards.
    AgentNode       Per-node agent: joins master, sends heartbeats, reports
                    live thermal metrics from a HuddleCluster instance.
    NodeRecord      Dataclass representing a registered node in the master.

Scheduler (v3.0.0)
-------------------
    ClusterScheduler  Thermal-fitness workload placement at the cluster level.
                      Pass scheduler=ClusterScheduler() to MasterNode to enable
                      GET /v1/scheduler/next, GET /v1/scheduler/stats, and
                      POST /v1/scheduler/report.

Auto Scaler (v3.1.0)
---------------------
    ClusterAutoScaler  Load-signal-based scale recommendations.
                       Pass autoscaler=ClusterAutoScaler() to MasterNode to enable
                       GET /v1/autoscaler/status and automatic on_scale_up /
                       on_scale_down callbacks.

Third-party backends (optional, require extra dependencies)
-----------------------------------------------------------
    RedisClusterBackend   Redis shared-state for multi-process deployments.
                          pip install "huddle-cluster[redis]"
    GrpcCluster           Thermal-aware gRPC channel routing.
                          pip install "huddle-cluster[grpc]"
    KubernetesDiscovery   Kubernetes pod auto-discovery via Watch API.
                          pip install "huddle-cluster[kubernetes]"

CLI
---
    After installation the ``huddle-cluster`` command is available:

        huddle-cluster master  start  [--port 7070]
        huddle-cluster agent   start  --id NODE_ID --master URL --port PORT
        huddle-cluster nodes   list
        huddle-cluster nodes   status NODE_ID
        huddle-cluster cluster status
        huddle-cluster cluster health
"""

from huddle_cluster_pkg.cluster_master      import MasterNode, NodeRecord
from huddle_cluster_pkg.cluster_agent       import AgentNode
from huddle_cluster_pkg.cluster_scheduler   import ClusterScheduler
from huddle_cluster_pkg.cluster_autoscaler  import ClusterAutoScaler

__all__ = [
    # Cluster management
    "MasterNode",
    "NodeRecord",
    "AgentNode",
    # Scheduler
    "ClusterScheduler",
    # Auto Scaler
    "ClusterAutoScaler",
]