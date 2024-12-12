The state machine in Raft receives committed logs and processes them. Raft logs can carry commands, which might be in a simple key-value format like `x=1`, meaning "set the value of key `x` to 1."  

To support a time-to-live (TTL) feature in the state machine, we need a way to record the time a log was appended and track the current time so the state machine can remove expired logs.  

---

### **Approach 1: Using the Leader's Timestamp**  
A straightforward method is to use the leader's local timestamp as the creation time for each new log. The leader attaches its timestamp to every appended log, and this timestamp is replicated across the cluster to followers.  

This approach makes it easy for both the leader and followers to remove expired logs, as they can use the attached timestamp to determine when each log should expire.  

**Example:**  
```
log 1: term: 1, timestamp: 10:00, ttl: 30 mins, cmd: x=1  
log 2: term: 1, timestamp: 10:05, ttl: 10 mins, cmd: y=2  
log 3: term: 1, timestamp: 10:30, ttl: 15 mins, cmd: z=3  
```  

At 10:30, when log 3 is fed into the state machine, it can safely remove keys `x` and `y`. The system knows that `x` expired at 10:30, and `y` expired at 10:15.  

However, this approach has a potential problem if the clocks of the leader and followers are not synchronized. For example, if the current leader crashes, and a follower with a clock drift becomes the new leader, it will attach its own (incorrectly shifted) timestamp to new logs.  

**Example:**  

Suppose the leader’s and followers’ clocks are out of sync, with the followers’ clocks 5 minutes ahead.  

Leader: 10:00  
Follower 1: 10:05  
Follower 2: 10:05  

Leader's logs:  
```
log 1: term: 1, timestamp: 10:00, ttl: 30 mins, cmd: x=1  
log 2: term: 1, timestamp: 10:05, ttl: 20 mins, cmd: y=2  
log 3: term: 1, timestamp: 10:10, ttl: 15 mins, cmd: z=3  
```  

At 10:15, the leader crashes, and a follower becomes the new leader. It appends a new log:  
```
log 4: term: 2, timestamp: 10:30, ttl: 30 mins, cmd: k=0  
```  

When log 4 is fed into the state machine, the current time is perceived as 10:30. Consequently, keys `x`, `y`, and `z` are incorrectly removed because they appear expired. In reality, the actual time is 10:25 due to the clock drift between the old and new leaders.  

This illustrates how this approach relies on synchronized clocks across nodes. To make this approach work reliably, time synchronization mechanisms (like NTP) must ensure that clock drift remains within acceptable limits.  

---

### **Approach 2: Using Cluster Time**  
Instead of recording the leader's local timestamp, we can track "cluster time"—the total time the cluster has been operating. Each log records the cluster time at the moment it is appended. If a leader crashes, the new leader picks up the cluster time from the last committed log and continues incrementing it.  

**Example:**  
```
log 1: term: 1, cluster-time: 1 min, ttl: 29 mins, cmd: x=1  
log 2: term: 1, cluster-time: 5 mins, ttl: 20 mins, cmd: y=2  
log 3: term: 1, cluster-time: 25 mins, ttl: 15 mins, cmd: z=3  
```  

- Log 1 is appended when the cluster has been operating for 1 minute. Its TTL is 29 minutes, so key `x` will expire when the cluster reaches 30 minutes.  
- Log 2 is appended when the cluster has been operating for 5 minutes. Its TTL is 20 minutes, so key `y` will expire when the cluster reaches 25 minutes.  
- When log 3 is fed into the state machine, the cluster time is 25 minutes, so key `y` is expired and removed.  

If the leader crashes and a follower becomes the new leader, the new leader continues counting from the last recorded cluster time.  

**Example:**  
```
--- Leader 1: started at 10:00  
log 1: term: 1, cluster-time: 1 min, ttl: 29 mins, cmd: x=1  
log 2: term: 1, cluster-time: 5 mins, ttl: 20 mins, cmd: y=2  
log 3: term: 1, cluster-time: 10 mins, ttl: 15 mins, cmd: z=3  
--- Leader 1 crashes at 10:15  

--- Leader 2: takes over at 10:20  
--- Leader 2 appends a new log at 10:35:  
log 4: term: 2, cluster-time: 25 mins, ttl: 30 mins, cmd: k=0  
```  

In this example:  
- Leader 1 records cluster time up to 10 minutes.  
- Leader 2, after taking over and operating for 15 minutes, appends a new log at cluster time 25 minutes.  

---

### **Issue: Accurate Log Expiration During Idle Periods**  
Both approaches face an issue when the leader is idle and does not receive new client logs.  

**Example:**  
```
--- Leader 1: started at 10:00  
log 1: term: 1, cluster-time: 5 mins, ttl: 5 mins, cmd: x=1  
log 2: term: 1, cluster-time: 30 mins, ttl: 5 mins, cmd: y=1  
```  

Log 1 is appended when the cluster has been operating for 5 minutes, with a TTL of 5 minutes. Key `x` should expire when the cluster reaches 10 minutes. However, since the state machine gets the current time only from logs, it must wait until log 2 is appended at 30 minutes to determine that `x` has expired. This results in key `x` being expired 20 minutes late.  

**What if we don’t wait for new logs to expire keys?**  

Consider this scenario:  
```
--- Leader 1: started at 10:00  
log 1: term: 1, cluster-time: 5 mins, ttl: 5 mins, cmd: x=1  
```  
- At 10:09, a client reads key `x` successfully.  
- At 10:10, key `x` is expired and removed.  
- At 10:12, the leader crashes.  
- At 10:13, a new leader takes over and replays the logs to rebuild the state machine.  
- At 10:14, a client reads key `x` successfully.  

This creates inconsistencies in serializable reads. For this reason, the system must wait for new logs to append and update the current time to ensure consistency.  

---

### **Mitigating the Issue**  
To address the problem during idle periods, we can append internal logs at regular intervals to record cluster time. This is controlled by a configuration parameter, `cluster_time_commit_max_duration`. For example, if this value is set to 30 seconds, the leader will append an internal log if no new client logs are received within 30 seconds.  

While this workaround improves TTL accuracy, it is not entirely precise. If a key's TTL is smaller than `cluster_time_commit_max_duration`, expiration may still be delayed.  

You can enhance TTL accuracy by reducing the configuration value (e.g., to 1 second), but this involves a trade-off: lower values hurt performance by increasing the frequency of log appends.
