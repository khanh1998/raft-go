# Raft Protocol Implementation and Applications
This project implements the Raft consensus protocol in Go, allowing developers to build distributed systems with fault-tolerant consensus mechanisms. The core Raft protocol is designed to be extensible, enabling the creation of custom extensions.

Currently, two extensions have been built on top of the Raft core:

- Classic: A simple in-memory key-value database.
- Etcd: Inspired by etcd 2.3, mimicking its interface and behavior.

## Raft implementation
This implementation includes all the core components described in the Raft paper:
- Leader Election: Ensures that one node is elected as the leader for decision-making and log replication.
- Log Replication: Ensures that logs are replicated across all nodes, providing consistency in the system.
- Safety: Guarantees that logs are never lost or overwritten, ensuring strong consistency.
- Client Interaction: Provides APIs for client requests, enabling users to interact with the distributed system.
- Cluster Membership Changes: Allows for dynamic changes in the cluster, including node additions and removals.
- Log Compaction: Implements snapshotting to truncate logs and keep the system efficient.

## Extensions
### [Classic (Key-Value DB)](extensions/classic/README.md)
Classic is a simple, hashmap-based key-value store built on top of the Raft protocol. It closely follows the original Raft paper to demonstrate how Raft can provide consistency in a basic database.
#### Key Features:
- Hash-based in-memory storage: Fast and lightweight key-value storage.
- Key-value operations: Supports `get`, `set`, `del`, and key-specific locking.
- Client session management: Includes `register` and `keep-alive` for client sessions with automatic request de-duplication.
- Static and dynamic clusters: Works with both fixed and adjustable cluster configurations.
- Consistency: Guarantees consistent reads and writes via Raft.

### [Etcd Clone (v2.3)](extensions/etcd/README.md)
The etcd clone replicates the interface and functionality of etcd 2.3, enabling seamless interaction with the cluster using tools like `curl`. This extension demonstrates how the Raft protocol can serve as the foundation for a distributed key-value store with a familiar and user-friendly API.

#### Key Features:
- etcd-inspired API: Implements functionality like `put`, `ttl`, `cas`, `get`, `delete`, `watch`, and more.
- B-tree-based in-memory storage: Supports efficient prefix-based get and delete operations.
- Static and dynamic clusters: Compatible with both fixed and adjustable cluster configurations.
- Consistency and fault tolerance: Ensures reliable and consistent operations using Raft.
> [!NOTE]  
> The API is inspired by etcd 2.3 but is not fully compatible with it. The etcd-clone uses a flat key structure but supports prefix-based operations to mimic folder-like functionality.

## Getting Started
### Prerequisites
- Go 1.22 or higher
- Dependencies: See `go.mod` for all required dependencies
- `make` is recommended
### Installation
1. Clone the repository: 
    ```bash
    git clone https://github.com/khanh1998/raft-go.git
    cd raft-go
    ```
2. Install dependencies:
    ```bash
    go mod tidy
    ```
### Running the Applications
**Running the Etcd Extension**
Navigate to the etcd extension directory and start a node:
```bash
cd extensions/etcd
make noded1
```
### Configuration
Each extension uses a `config.yml` file for configuration, including cluster size, ports, and other options. Ensure the file is properly configured before starting the applications.
- [Raft core configuration](raft_core/README.md)

### Client Communication
Both extensions use HTTP for client communication.
**Example: Interacting with the Etcd Extension**
You can use curl to interact with the etcd extension. For example, to get a key:
```bash
curl -L http://localhost:8080/v2/keys/cs/db/sql/mysql
```

## Building Your Own Extension
This project is designed to be extensible. You can create your own applications based on the Raft core by implementing a new extension under the `extensions/` directory. Use the existing `extensions/classic` and `extensions/etcd` examples as a reference.

## Contributing
Contributions are welcome! If you have ideas for improvements or new extensions, feel free to open an issue or submit a pull request.

Steps to contribute:
1. Fork the repository.
2. Create a feature branch:
    ```bash
    git checkout -b feature/new-extension
    ```
3. Commit your changes:
    ```bash
    git commit -m "Add new feature"
    ```
4. Push to your branch and open a pull request.
