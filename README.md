This is a simple distributed key-value database, built on top of [Raft](https://raft.github.io/) consensus algorithm. At the moment, this raft cluster now supports:
- Leader election
- Log replication
- Safety
- Client interaction
- Cluster membership changes (in progress)
- Log compaction (pending)

![Orverall Architecture](docs/diagram.drawio.png "Orverall Architecture")

# 1. Start the cluster
## 1.1 Using multiple terminals (recommended)
In this mode, logs of each node in the cluster will be shown in its terminal.\
To clear the previous state of the cluster: `make clear`\
Open three terminals, and input the three below commands to three terminals respectively:\
Terminal 1 - Node 1: `make node1`\
Terminal 2 - Node 2: `make node2`\
Terminal 3 - Node 3: `make node2`
## 1.2 Using one terminal
In this mode, logs of nodes in the cluster will be all shown in one terminal.\
To clear the previous state of the cluster: `make clear`\
Open a terminal, and type: `make run`
# 2. Specify the URL of the leader
After the cluster is started up, a few seconds later there will be an election, and finally a leader will be elected.\
The default cluster will have three nodes, initially, all three nodes are followers, and after a successful election, one of the followers becomes the leader of the cluster.\
At the moment, all of your requests including reading and writing need to go through the leader.\
To find which node is the current leader, you can check its log, recent logs of the leader will have `state=leader`. At the moment node's HTTP URL is hardcoded in `main.go`.
# 3. Interact with the cluster
## 3.1 Register client
This API is to create a session for a current client. To read or write data to the cluster, a session is required. The purpose of the session is to make the requests are idempotent, by assigning a monotonically increasing number.

You need to replace the `localhost:8080` with the leader URL you found in step #2.
```bash
curl --location 'localhost:8080/cli' \
--header 'Content-Type: application/json' \
--data '{
    "client_id": 0,
    "sequence_num": 0,
    "command": "register"
}'
```

If you send the request to the leader, and the request is successful, you will receive a response like the one below. `response` is the id of your session, you need to include this session id in later requests.
```json
{
    "status": "OK",
    "response": 2,
    "leader_hint": ""
}
```

If you send a request to a follower, you most likely received a response which is similar to this:

```json
{
    "status": "Not OK",
    "response": 0,
    "leader_hint": "localhost:8080"
}
```

## 3.2 Send command to cluster
This is a key value database, so you can send a command to the leader to set (or overwrite) the value for a specific key.

- The format of the `command`: [set][a space][key-name][a space][value need to be set]
- Session ID `client_id`: You got this ID in the `response` from step #3.1
- Sequence Number `sequence_num`: The purpose of sequence num is to ensure the idempotent of the request, so you can retry it safely.
After each command you need to increase the sequence num, You need to make sure the sequence num of the later request must be bigger than the former one. \
In case you want to retry the command, keep the sequence number the same.
```bash
curl --location 'localhost:8080/cli' \
--header 'Content-Type: application/json' \
--data '{
    "client_id": 2,
    "sequence_num": 1,
    "command": "set name quoc khanh"
}'
```
If the request goes through the leader and gets processed successfully, the response will be like this:
```json
{
    "status": "OK",
    "response": "quoc khanh",
    "leader_hint": ""
}
```
If you send a request to a follower, you will receive:
```json
{
    "status": "Not OK",
    "response": "NOT_LEADER",
    "leader_hint": "localhost:8080"
}
```
## 3.3 Query data
After set the value for the keys in the previous step, you can now query it:\
Query format: [get][a space][key-name]
```bash
curl --location 'localhost:8080/cli' \
--header 'Content-Type: application/json' \
--data '{
    "client_id": 0,
    "sequence_num": 0,
    "command": "get name"
}'
```
Since we don't need `client_id` and `sequence_num` in this request, so we can set it both as `0`.

If success:

```json
{
    "status": "OK",
    "response": "quoc khanh",
    "leader_hint": ""
}
```

Failed:

```json
{
    "status": "Not OK",
    "response": "NOT_LEADER",
    "leader_hint": "localhost:8080"
}
```