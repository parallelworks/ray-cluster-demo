# Multi-Site Ray Cluster Demo

Unified compute fabric across two ACTIVATE resources using [Ray](https://ray.io). Starts a Ray head node on an on-prem resource, connects Ray workers from a cloud resource via reverse SSH tunnel, runs a distributed benchmark across the unified cluster, and shows a live dashboard with cluster topology and task placement.

## Architecture

```
              ACTIVATE Workflow
                    |
         +----------+----------+
         v                      v
   On-Prem (SSH)          Cloud (Slurm)
   Ray HEAD node          Ray WORKER nodes
   + Custom Dashboard     connect via tunnel
         |                      |
         +---- Ray Cluster -----+
                    |
            Distributed Benchmark
            (tasks auto-scheduled across sites)
                    |
              Live Dashboard
              (topology, task placement, throughput)
```

## Quick Start

1. Add this workflow to your ACTIVATE account
2. Select two resources:
   - **On-prem**: Always-on resource (e.g., `a30gpuserver`) — runs Ray head + dashboard
   - **Cloud**: Cloud cluster (e.g., `googlerockyv3`) — runs Ray workers
3. Click **Execute**
4. Open the session proxy link to view the live dashboard

## Benchmark Phases

1. **Task Throughput** — Bursts many small tasks to measure Ray scheduling rate and task placement distribution across sites
2. **CPU Compute** — Matrix multiplications (NumPy) measuring GFLOPS per node
3. **Scaling Test** — Compares throughput using the full 2-site cluster vs estimated single-site performance

## Dashboard Views

- **Cluster Topology** — Nodes grouped by site with CPU counts
- **Task Placement** — Live bar chart showing tasks completed per site
- **Throughput Chart** — Tasks/sec over time, stacked by site
- **Benchmark Results** — GFLOPS, scaling speedup, task latency

## How It Works

Ray workers on the cloud resource need to reach the head node's GCS port (6379). Since cloud and on-prem are on different networks, the workflow creates a reverse SSH tunnel:

1. `setup_tunnel.sh` creates a reverse tunnel from on-prem to cloud, forwarding both the Ray GCS port and the dashboard port
2. Cloud workers run `ray start --address=localhost:TUNNEL_PORT` to join the cluster through the tunnel
3. All Ray traffic flows through SSH transparently

## Files

```
ray-cluster-demo/
├── workflow.yaml                # Multi-site workflow (2 resources, 9 jobs)
├── thumbnail.png                # Workflow thumbnail
├── README.md                    # This file
└── scripts/
    ├── setup.sh                 # Install Ray + NumPy via uv/pip
    ├── start_ray_head.sh        # Start Ray head + dashboard on on-prem
    ├── start_ray_workers.sh     # Connect cloud workers to Ray head via tunnel
    ├── setup_tunnel.sh          # Reverse tunnel for Ray GCS + dashboard ports
    ├── run_benchmark.sh         # Run benchmark, POST results to dashboard
    ├── benchmark.py             # Ray distributed benchmark tasks
    ├── dashboard.py             # FastAPI live dashboard server
    ├── generate_thumbnail.py    # Thumbnail generator
    └── templates/
        └── index.html           # Dashboard UI (topology + charts)
```
