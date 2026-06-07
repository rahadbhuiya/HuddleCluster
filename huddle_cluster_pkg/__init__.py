"""
huddle_cluster_pkg — Optional extension modules for HuddleCluster.

Modules
-------
backends_redis   Redis shared-state backend for multi-node deployments.
grpc_cluster     Thermal-aware gRPC channel routing.
discovery_k8s    Kubernetes pod auto-discovery via the Watch API.

Each module declares its own external dependency and raises a clear
ImportError with an install hint when that dependency is absent.

Install extras
--------------
    pip install "huddle-cluster[redis]"
    pip install "huddle-cluster[grpc]"
    pip install "huddle-cluster[kubernetes]"
    pip install "huddle-cluster[redis,grpc,kubernetes]"
"""