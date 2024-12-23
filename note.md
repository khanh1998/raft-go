questions:
- how to handle a server that has been left far behind. let it sync with the leader or remove it.
    given three nodes cluster, one node has die, one node has latest log, one node has lagged behind.
    the node with latest log will obviously become leader, its job now is to sync logs to the lagged one.
    the cluster required that before election timeout, the leader have to have one round of heartbeat success.
    let's say the lagged node is so far a way, time to sync that node will take more than one election timeout duration.
    more specifically, time to find the starting conflict point take most of the time.
    that will make the current step down, without sync any log to the lagged one.
    the next time that node step up as leader, it have to do thing again from beggining.
    we need a faster algo to find the conflic point.

- given five nodes: A, B, C, D, and E. let's assume Node A is the current leader with term 10. nodes B, C, D, and E are followers. Node E gets isolated from the rest of the cluster because of network partition.
    now, network partition resolves, node E increases it's term to 11, and requests vote.
    because node A (leader) has lower term, so when receive the request vote, it steps down, to become follower.
    the request vote from node E won't effect the other followers (B, C, D), because these follower won't grant vote to any node until an election timeout elapsed.
    after the node A, step down, it will request vote again and become leader again, as it has most up to date log.

todos:
- support log compaction
- using try-lock instead
- shorten critical region in outbound

- using observer design patter:
    state machine -> committed log
    client -> committed log
    membership -> add/remove server
    stop channel

- write a db engine
- upgrade rpc to gRPC
- get data from follower (not up-to-date data)
- do benchmark
- need a new method that let a new node can join the cluster easier
- concurrency testing
- async set, don't wait for log commit
- allow node with some data to catch up, currently only completely new node (no data) can.
- vault