# run node 1 first, let's it form a one-node-cluster
node1:
	go run -race main.go -id=1 -catching-up=false -rpc-port=1234 -http-port=8080
# then add node 2 and 3 to the cluster
node2:
	go run -race main.go -id=2 -catching-up=true -rpc-port=1235 -http-port=8081
node21:
	go run -race main.go -id=2 -catching-up=false -rpc-port=1235 -http-port=8081
node3:
	go run -race main.go -id=3 -catching-up=true -rpc-port=1236 -http-port=8082
# all the next nodes have to set -catching-up=true

nodes1:
	go run main.go -id=1
nodes2:
	go run main.go -id=2
nodes3:
	go run main.go -id=3
	
clear:
	rm -f log.*.dat
