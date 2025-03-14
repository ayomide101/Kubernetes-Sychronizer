# K8s Sync CLI

K8s Sync CLI is a Go-based command-line application designed to replicate Kubernetes resources from a primary cluster to one or more replica clusters. It supports both continuous watch mode (where it listens for events and replicates changes as they occur) and one-time sync mode (which performs an immediate, verbose synchronization) and includes built-in Prometheus metrics, a simple REST API for monitoring and management, and SQLite-based persistence for job tracking and user accounts.. The tool is highly configurable, allowing you to select which resource types to sync, restrict syncing to specific namespaces, and even skip sensitive namespaces by default.


## Overview

K8s Sync CLI is designed to simplify the process of replicating resource changes across multiple Kubernetes clusters. It leverages the Kubernetes client-go library to watch for events in a primary cluster and replicate them to configured replica clusters. The application runs in two modes:
- **Watch Mode (default):** Continuously listens for updates on the primary cluster and replicates changes asynchronously.
- **One-Time Sync Mode:** Performs an immediate, verbose sync of all resources across specified namespaces (ensuring namespaces exist on replicas first) and then exits.

## Features

- **Resource Syncing:**  
  Replicates the following Kubernetes resource types:
    - Deployments
    - StatefulSets
    - ConfigMaps
    - Secrets
    - PersistentVolumeClaims (PVCs)
    - Services
    - Ingresses
  - Supports both continuous watch and one-time sync modes.
  
- **Namespace Filtering:**  
  - By default, skips sensitive namespaces (e.g. namespaces starting with `kube`) unless overridden.
  - Supports syncing of only specified namespaces via a flag.

- **Persistence & Tracking:**  
  - Uses SQLite to store sync jobs, their statuses, and user accounts.
  - Prevents duplicate replication by computing a unique change key per resource change.

- **REST API & Metrics:**  
  - Provides endpoints for login, dashboard metrics, cluster status, and retrying failed replications.
  - Exposes Prometheus metrics on `/metrics`.

- **RBAC & Security:**  
  - Includes RBAC manifests (to be used with Kubernetes deployments) that allow the application to watch and read resources across multiple namespaces.
  - Built-in authentication using SQLite-backed user accounts with bcrypt password hashing.

- **SQLite Persistence**  
  Sync jobs and user accounts are stored in a local SQLite database. Duplicate replication is prevented by computing a unique change key per resource event.


## Installation

### Prerequisites

- [Go 1.18+](https://golang.org/dl/)
- A working Kubernetes cluster (for primary)
- Replica cluster kubeconfig files
- [SQLite3](https://www.sqlite.org/download.html) (or use a pre-built Docker image)

### Building

Clone the repository and build the binary:

```bash
git clone https://github.com/ayomide101/k8s-sync-cli.git
cd k8s-sync-cli
go build -o k8s-sync ./cmd/k8s-sync
```

### Configuration & Usage

#### CLI Flags
The application is configured via CLI flags:
	•	--primary: Path to the primary cluster’s kubeconfig file.
	•	--replica-dir: Directory containing kubeconfig files for replica clusters.
	•	--worker-count: Number of worker goroutines for processing sync tasks.
	•	--namespaces: Comma-separated list of namespaces to sync. If empty, all non-sensitive namespaces are synced by default.
	•	--sync-once: Run one-time sync mode (verbose) and exit.
	•	--allow-sensitive-namespaces: Allow syncing of sensitive namespaces (names starting with kube).
	•	--features: Comma-separated list of resource types to sync. Default: deployments,statefulsets,configmaps,secrets,pvcs,services,ingresses (You can override this to sync only specific features.)


For example:

```bash
./k8s-sync --primary=/path/to/primary.kubeconfig --replica-dir=/path/to/replicas --sync-namespaces=default,production --worker-count=5
```

### Usage

Watch Mode

By default, the application runs in continuous watch mode. It starts Kubernetes informers for the enabled features and listens for resource changes on the primary cluster. When a change occurs, it replicates the update to each replica cluster asynchronously.

To start in watch mode:

```bash
./k8s-sync --primary=/path/to/primary.kubeconfig --replica-dir=/path/to/replica/kubeconfigs
```


### One-Time Sync Mode

One-time sync mode immediately replicates all enabled resource types across all (or specified) namespaces. It first lists the namespaces to sync (skipping sensitive ones unless overridden), ensures that each namespace exists on every replica, and then processes each resource type within that namespace. Detailed progress is printed to the CLI.

To run one-time sync mode:

```bash
./k8s-sync --primary=/path/to/primary.kubeconfig --replica-dir=/path/to/replicas --sync-once
```

In one-time sync mode, the application:
	1.	Lists namespaces (skipping sensitive ones unless overridden).
	2.	Ensures each namespace exists on the replica clusters.
	3.	Iterates through each namespace and syncs Deployments, StatefulSets, ConfigMaps, Secrets, and PersistentVolumeClaims.
	4.	Provides detailed progress output to the CLI.

### REST API Endpoints

The application exposes a basic REST API on port 8080:
	•	POST /api/login:
Accepts JSON credentials to authenticate a user.
	•	GET /api/dashboard:
Returns aggregated job metrics (total, ongoing, failed, and pending sync jobs).
	•	GET /api/status:
Returns the connectivity status and server version for the primary and replica clusters.
	•	POST /api/users:
Allows creation of new user accounts (requires valid credentials).
	•	GET /retry:
Allows retrying a failed replication for a specific job on a specified replica (via query parameters jobid and replica).

### Metrics
Prometheus metrics (job count, latency, success/failure rates) are exposed via the /metrics endpoint.

## Testing

Run the test suite using:

```bash
go test -v ./...
```

An extensive set of tests covers one-time sync functionality, duplicate prevention, error handling, and resource replication.

## Contributing

Contributions are welcome! Please open issues and submit pull requests for improvements and bug fixes. Follow the standard Go project guidelines and ensure tests pass.

## License

This project is licensed under the MIT License. See the [LICENSE] file for details.
