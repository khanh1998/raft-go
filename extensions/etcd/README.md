In this clone, directory-like storage is not supported as in the original Etcd 2.3. However, it supports prefixes to mimic directory storage behavior. For example, you can query or delete all keys with a specific prefix.

# **Configuration**
The Etcd extension's configuration is located under the `extension` section in `config.yaml`. Below is an explanation of the available options:

- **`state_machine_history_capacity`**  
  Each time the state machine is modified, it emits an event. Recent events are cached, and this configuration determines the size of that cache.  
  **Recommended size:** `1000` to `2000`.

- **`state_machine_btree_degree`**  
  The state machine stores key-value pairs internally using a BTree structure to support efficient key range queries. This setting specifies the degree of the BTree.
  **Recommended degree:** `32` to `128`.

- **`http_client_max_wait_timeout`**  
  This extension supports clients waiting for changes to specific keys using long polling. This configuration specifies the maximum duration a client can wait for a key update.
  **Recommended value:** `60m`.
# Start the Cluster
Change the directory to the Etcd extension
```
cd extensions/etcd
```

## Using Docker Compose
If you want to quickly interact with the cluster:
1. Launch the environment: `docker compose up -d --build`
2. Interact via the `curl` container: `docker compose exec -it curl sh`

### Important Consideration
When sending requests to cluster nodes:
- Use the Curl container within the Docker network to avoid issues with HTTP 308 redirect responses.
- Avoid sending requests directly from your host machine, as it won't recognize cluster node hostnames (e.g., `raft-node-3.raft-cluster.local`) due to redirect responses.

This ensures seamless communication with the cluster.
Example usage:
```bash
curl -L raft-node-3.raft-cluster.local:8080/v2/keys/message
```
Hostnames for cluster nodes follow the format `raft-node-\<number>.raft-cluster.local`, as specified in the Docker configuration.
## Static cluster

Open three terminals corresponding to three nodes of cluster, if you want to add more nodes, then you need to change the `config.yml`.
Terminal 1 - Node 1: `make nodes1`\
Terminal 2 - Node 2: `make nodes2`\
Terminal 3 - Node 3: `make nodes2`
Wait (~30s) for the cluster to elect a leader, indicated by `state=leader` in the logs.

## Dynamic cluster
### Create cluster
#### Create Initial Node
1. Create first terminal, and run: `make noded1`, or more specifically:
    ```bash
    go run main.go -id=1 -catching-up=false -rpc-port=1234 -http-port=8080
    ```
2. Wait util this node become leader, by checking `state` field in the log.
    - This will create a one-node cluster, with the first node is leader. 
    - You can start to interact with this cluster now, or add more nodes to it.
#### Add more node to cluster
##### Create new node
Create a new terminal for each new node you want to add to cluster, and run:
```bash
go run main.go -id=[NEW_ID] -catching-up=true -rpc-port=[NEW_RPC_PORT] -http-port=[NEW_HTTP_PORT]
```
Replace `[NEW_ID]`, `[NEW_RPC_PORT]`, and `[NEW_HTTP_PORT]` with unique values.
You now have a new node, but it hasn't joined the cluster yet.
##### Joining the Node to the Cluster
Send this request to leader: 
```bash
curl -XPOST http://localhost:8080 \
  --data-urlencode "serverId=[NEW_ID]" \
  --data-urlencode "httpUrl=localhost:[NEW_HTTP_PORT]" \
  --data-urlencode "rpcUrl=localhost:[NEW_RPC_PORT]"
```

# Tools to interact with the cluster
You can use `curl` or UI tools like Postman to interact with the cluster. Note that every request must go through the cluster leader. If you send a request to a follower, it will respond with a 308 status code and provide the leader's URL in the `Location` header. Some tools automatically follow the provided URL and redirect to the leader.
## Using curl
When using `curl`, include the `-L` option for automatic redirection to the leader. Without `-L`, you'll receive a 308 response:
```bash
curl -i http://localhost:8080/v2/keys/message
```
Response:
```json
HTTP/1.1 308 Permanent Redirect
...
Location: http://localhost:8081/v2/keys/message
X-Raft-Index: 2
X-Raft-Term: 1
...
{
    "cause": "raft",
    "errorCode": 0,
    "index": 0,
    "message": "NOT_LEADER"
}
```
Cluster leader's hostname in this case is `localhost:8081`.
# API
## Key API
### Create a key
Set a key-value pair:
```bash
curl -L http://localhost:8080/v2/keys/message -XPUT -d value="Hello world"
```
Response:
```json
{
    "action": "set",
    "node": {
        "key": "message",
        "value": "Hello world",
        "modifiedIndex": 3,
        "createdIndex": 3
    },
    "prevNode": {}
}
```
#### Response fields
- `action`:
    - `get`: when you send `GET` to get a key
    - `create`: when you send `POST` or `PUT`-with-`prevExist=false`
    - `set`: when you send `PUT`
    - `update`: when you send `PUT` with `prevExist=true`
    - `compareAndSwap`: when you send `PUT` with `prevValue` or (and) `prevIndex`
    - `delete`: when you send `DELETE`
    - `compareAndDelete`: when you send `DELETE` with `prevValue` or (and) `prevIndex`
    - `expired`: when your keys are expired
- `node`: contains current value of the key
    - `createdIndex`: index at which the key first created
    - `modifiedIndex`: latest index at which the key get updated (>= `createdIndex`)
- `prevNode`: contains previous version of the key if any

#### Response headers:
Addition headers are returned in header for almost every request:
```
X-Etcd-Index: 2
X-Raft-Index: 17
X-Raft-Term: 7
```

- `X-Raft-Index`: raft's latest committed log index
- `X-Raft-Term`: raft's current leader term
- `X-Etcd-Index`: tracks state machine changes, incrementing with each successful key update, creation, deletion or expiration.
These headers are returned for most requests. `X-Etcd-Index` can be used with the wait command.

### Get key values
#### Get a key
Retrieve a key's value:
```bash
curl -L -i http://localhost:8080/v2/keys/message
```
Response:
```json
{
    "action": "get",
    "node": {
        "key": "message",
        "value": "Hello world",
        "modifiedIndex": 3,
        "createdIndex": 3
    },
    "prevNode": {}
}
```
Key not found (404):
```json
{
    "cause": "message",
    "errorCode": 404,
    "index": 0,
    "message": "key does not exist"
}
```
### Get keys by prefix
Get keys with a shared prefix:
```bash
curl -L -i http://localhost:8080/v2/keys/message?prefix=true
```
Response:
```json
{
    "action": "get",
    "node": {},
    "nodes": [
        {
            "key": "message/external",
            "value": "content",
            "modifiedIndex": 17,
            "createdIndex": 17
        },
        {
            "key": "message/internal",
            "value": "content",
            "modifiedIndex": 16,
            "createdIndex": 16
        }
    ],
    "prevNode": {}
}
```

### Set value for a key
Set a key-value pair:
```bash
curl -L http://localhost:8080/v2/keys/message -XPUT -d value="Hello you"
```
Response:
```json
{
    "action": "set",
    "node": {
        "key": "message",
        "value": "Hello you",
        "modifiedIndex": 4,
        "createdIndex": 3
    },
    "prevNode": {
        "key": "message",
        "value": "Hello world",
        "modifiedIndex": 3,
        "createdIndex": 3
    }
}
```
Response fields:
- `prevNode`: stores the previous key value.
- `modifiedIndex`: increments by 1 with each change.
### Deleting key
#### Deleting a key:
```bash
curl -L http://localhost:8080/v2/keys/message -XDELETE
```
Response:
```json
{
    "action": "delete",
    "node": {
        "key": "message",
        "modifiedIndex": 5,
        "createdIndex": 3
    },
    "prevNode": {
        "key": "message",
        "value": "Hello you",
        "modifiedIndex": 4,
        "createdIndex": 3
    }
}
```
#### Deleting Keys with Prefix
Delete keys with a shared prefix:
```bash
curl -L http://localhost:8080/v2/keys/message?prefix=true -XDELETE
```
Response:
```json
{
    "action": "delete",
    "node": {
        "key": "message",
        "modifiedIndex": 19,
        "createdIndex": 19
    },
    "prevNode": {},
    "prevNodes": [
        {
            "key": "message/external",
            "value": "content",
            "modifiedIndex": 19,
            "createdIndex": 19
        },
        {
            "key": "message/internal",
            "value": "content",
            "modifiedIndex": 18,
            "createdIndex": 18
        }
    ]
}
```

### Setting key TTL
#### TTL value format
Go duration string (e.g., `300ms`, `-1.5h`, `2h45m`)
##### Valid Units
- `ns` (nanoseconds)
- `us` (microseconds)
- `ms` (milliseconds)
- `s` (seconds)
- `m` (minutes)
- `h` (hours)

Set a key's value with a 5-minute expiration:
```bash
curl -L http://localhost:8080/v2/keys/lock -XPUT -d value=1 -d ttl=5m
```
Response:
```json
{
    "action": "set",
    "node": {
        "key": "lock",
        "value": "1",
        "modifiedIndex": 5,
        "createdIndex": 5,
        "expirationTime": 1719768803959
    },
    "prevNode": {}
}
```
Notes:
1. `expirationTime` reflects cluster time, utilizing the [cluster clock](../../raft_core/cluster-clock.md) mechanism.
2. TTL accuracy is limited; avoid durations < 1 minute.
3. Expired keys return 404 errors.
4. When a key expires, clients waiting for key updates receive a expiration notification.

#### Unset TTL
Remove expiration by setting `ttl=0`, ensuring the key never expires:
```bash
curl -L http://localhost:8080/v2/keys/lock -XPUT -d value=2 -d ttl=0 -d prevExist=true
```
Response:
```json
{
    "action": "update",
    "node": {
        "key": "lock",
        "value": "2",
        "modifiedIndex": 11,
        "createdIndex": 9
    },
    "prevNode": {
        "key": "lock",
        "value": "2",
        "modifiedIndex": 10,
        "createdIndex": 9,
        "expirationTime": 2205896462252
    }
}
```
The `expirationTime` field disappears, indicating persistence.

### Refreshing key TTL
Update a key's expiration time without modifying its value.
#### Restriction
1. Key's value remains unchanged.
2. TTL refreshes occur silently, without notifying clients waiting for the key's changes.
#### Usage
```bash
curl http://localhost:8080/v2/keys/lock -XPUT -d refresh=true -d ttl=5m -d prevExist=true
```
##### Response
```json
{
    "action": "update",
    "node": {
        "key": "lock",
        "value": "1",
        "modifiedIndex": 14,
        "createdIndex": 13,
        "expirationTime": 3277345671794
    },
    "prevNode": {
        "key": "lock",
        "value": "1",
        "modifiedIndex": 13,
        "createdIndex": 13,
        "expirationTime": 2952723656628
    }
}
```

### Waiting for a change
Utilize long polling to watch keys for updates:
#### Watch
1. Terminal 1: Watch key with `wait=true`:
    ```bash
    curl -L http://localhost:8080/v2/keys/lock?wait=true
    ```

2. Terminal 2: Update key:
    ```bash
    curl -L http://localhost:8080/v2/keys/lock -XPUT -d value=2 -d prevExist=true
    ```
##### Response:
In the terminal 1, you will see:
```json
{
    "action": "update",
    "node": {
        "key": "lock",
        "value": "2",
        "modifiedIndex": 15,
        "createdIndex": 13,
        "expirationTime": 3277345671794
    },
    "prevNode": {
        "key": "lock",
        "value": "1",
        "modifiedIndex": 14,
        "createdIndex": 13,
        "expirationTime": 3277345671794
    }
}
```
#### Continuous watching
After receiving an event, immediately send another wait request to capture subsequent events with `waitIndex={modifiedIndex + 1}`:
```bash
curl -L http://localhost:8080/v2/keys/lock?wait=true&waitIndex=16
```
#### Event history cache
After receiving an event, immediately send another wait request to continue capturing events. However, there's a risk of missing events between requests. To mitigate this, the state machine caches recent events. After receiving an event, start waiting on the `modifiedIndex + 1`.
The event cache has a default capacity of 1000, configurable via `config.yml` (`state_machine_history_capacity`).

#### Cleared event
Since the cache is limited, listening on an outdated index yields an error:
```bash
curl -L http://localhost:8080/v2/keys/lock\?wait\=true\&waitIndex\=1
```
Response:
```json
{
    "cause": "the requested history has been cleared [2/1]",
    "errorCode": 401,
    "index": 11,
    "message": "The event in requested index is outdated and cleared"
}
```                                                              
This response indicates oldest waitable index is 2.

#### Premature connection closed
This wait request utilizes long polling, which may result in premature request closure. Plan accordingly when building your application.

### Compare and swap
Perform conditional updates using:
#### Options
- `prevExist`: Require key existence (true) or non-existence (false).
- `prevValue`: check the current key's value
- `prevIndex`: check the current key's `modifiedIndex`
#### Usage
```bash
curl -L http://localhost:8080/v2/keys/lock -XPUT -d value=4 -d prevExist=true -d prevValue=3
```
#### Response
##### Success
```json
{
    "action": "update",
    "node": {
        "key": "lock",
        "value": "4",
        "modifiedIndex": 4,
        "createdIndex": 1
    },
    "prevNode": {
        "key": "lock",
        "value": "3",
        "modifiedIndex": 3,
        "createdIndex": 1
    }
}
```
#### Failure
If your preconditions are not met, error will be returned:
```json
{
    "cause": "[prevExist=true,prevValue=2]",
    "errorCode": 400,
    "index": 5,
    "message": "compare failed"
}
```

### Compare and delete
Delete keys conditionally using:
#### Options
- `prevValue`: check the current value of the key and delete
- `prevIndex`: check the current `modifiedIndex` of the key and delete
#### Usage
```bash
curl -L http://localhost:8080/v2/keys/lock\?prevValue\=3 -XDELETE
```
#### Response
##### Compare failure:
```json
{
    "cause": "[prevValue=3]",
    "errorCode": 400,
    "index": 61,
    "message": "compare failed"
}
```
###### Success:
```json
{
    "action": "compareAndDelete",
    "node": {
        "key": "lock",
        "modifiedIndex": 13,
        "createdIndex": 12
    },
    "prevNode": {
        "key": "lock",
        "value": "2",
        "modifiedIndex": 13,
        "createdIndex": 12
    }
}
```
### Uploading text files to a keys
Utilize tools like curl or Postman to upload text files to keys.

#### Using curl:
1. Save your content to a file, for example:
    ```bash
    echo '{                                                          
        "cause": "lock",
        "errorCode": 404,
        "index": 11,
        "message": "key does not exist"
    }' > dat.json
    ```
2. Upload the file
    ```bash
    curl -L http://localhost:8080/v2/keys/config -XPUT --data-urlencode value@dat.json
    ```
3. Response:
    ```json
    {
        "action": "set",
        "node": {
            "key": "config",
            "value": "{\n    \"cause\": \"lock\",\n    \"errorCode\": 404,\n    \"index\": 11,\n    \"message\": \"key does not exist\"\n}\n",
            "modifiedIndex": 14,
            "createdIndex": 14
        },
        "prevNode": {}
    }
    ```
#### Using Postman
1. Select `PUT` request.
2. Enter URL, for example: http://localhost:8080/v2/keys/config.
3. Choose `form-data`.
4. Add key `value` with type `File` and select your file (e.g., dat.json).

Equivalent Curl command for Postman:
```bash
curl --location --request PUT 'http://localhost:8080/v2/keys/config' \
--form 'value=@"dat.json"'
```

## Member API
This API is only applicable for dynamic clusters. Ensure your config.yml file sets mode to `dynamic`.

### Adding/Removing Cluster Nodes
Manage nodes by sending requests to the leader:
(assume that your leader's hostname is `localhost:8080`)
#### Add node
You need to start you new node up first, then:
```bash
curl -XPOST http://localhost:8080 \
  --data "serverId=<ID>&httpUrl=<HTTP_URL>&rpcUrl=<RPC_URL>"
```
#### Remove node
```bash
curl -XDELETE http://localhost:8080 \
  --data "serverId=<ID>&httpUrl=<HTTP_URL>&rpcUrl=<RPC_URL>"
```
#### Request Parameters
- `serverId`: Unique identifier for the new node
- `httpUrl`: HTTP URL of the new node
- `rpcUrl`: RPC URL of the new node
Replace `<ID>`, `<HTTP_URL>`, and `<RPC_URL>` with the actual values for the node you want to add or remove.