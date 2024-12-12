The `raft_core` section in `config.yaml` contains configurations for the Raft protocol. Below is a breakdown of the available options:

### **Cluster Configuration**
- **`cluster`**
  - **`mode`**  
    Specifies the cluster's operational mode:
    - `static`: The cluster has a fixed set of members (servers). Nodes cannot be added or removed, and the quorum remains constant.
    - `dynamic`: Nodes can be added or removed using the Member API. When the cluster membership changes, the quorum adjusts accordingly.
  - **`servers`**  
    Lists the cluster's members in `static` mode. In `dynamic` mode, member information is added dynamically via the Member API.

### **Timeout Settings**
- **`min_election_timeout`** and **`max_election_timeout`**  
  Define the range for the election timeout. These values should typically be 5 to 10 times the heartbeat duration.  
  **Recommended values:** `250ms` to `500ms`.
  
- **`min_heartbeat_timeout`** and **`max_heartbeat_timeout`**  
  Specify the range for the heartbeat timeout. These values should be 0.5 to 1.5 times the round-trip time between nodes.  
  **Recommended values:** `50ms` to `100ms`.

### **Storage Configuration**
- **`data_folder`**  
  Specifies the folder where Write-Ahead Logs (WAL) and snapshot files are stored.

- **`wal_size_limit`**  
  When a WAL file reaches the specified size, a new file is created. Older WALs can be deleted during the snapshot process.

- **`log_length_limit`**  
  Defines the maximum number of logs to keep in memory. If this limit is exceeded, the system initiates a snapshot to compact the logs. Keeping too many logs in memory can consume resources and degrade performance.

### **Internal RPC Settings**
- **`rpc_dial_timeout`**  
  The maximum duration for establishing an RPC connection between nodes in the cluster. The operation will be canceled if it exceeds this duration.

- **`rpc_request_timeout`**  
  The maximum duration for completing an RPC request. Requests exceeding this duration will be canceled.

- **`rpc_reconnect_duration`**  
  If a node fails to send a request to another node, it will close the connection and attempt to reconnect after the specified duration.

### **Snapshot Settings**
- **`snapshot_chunk_size`**  
  When a new node joins a dynamic cluster, it lacks data. The leader sends the latest data snapshot to the new node. Since snapshots are typically large, they are sent in smaller chunks. This setting specifies the size of each chunk.

### **Cluster Time Configuration**
- **`cluster_time_commit_max_duration`**  
  This configuration relates to the concept of the [cluster clock](cluster-clock.md).  
  - The cluster clock tracks how long the cluster has been running (in nanoseconds).  
  - Each time the leader appends a new log, it attaches the cluster time to the log and persists it to the WAL.  
  - If no new logs are received from clients, the cluster time is not recorded, which can impact TTL-based operations.  
  - To address this, the leader periodically appends internal logs to record the current cluster time.  

This periodic logging is essential for maintaining TTL accuracy. A lower value for `cluster_time_commit_max_duration` improves TTL precision but can negatively affect performance if set too low.

### **HTTP Client Settings**
- **`http_client_request_max_timeout`**  
  Specifies the maximum duration for HTTP requests from clients. Requests that exceed this duration will be canceled.