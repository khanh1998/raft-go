# static mode
all nodes in cluster use the same fixed http and rpc port
we also pass in the size of cluster
since we deploy cluster by utilizing the stateful set, each node can connect to other using headless service


# dynamic mode
http and rpc ports are fixed like static mode