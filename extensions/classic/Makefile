# run node 1 first, let's it form a one-node-cluster
noded1:
	go run main.go -id=1 -catching-up=false -rpc-port=1234 -http-port=8080
# then add node 2 and 3 to the cluster
# after the node get added to the cluster, set catching-up=false, if you ever restart the node.
noded2:
	go run main.go -id=2 -catching-up=true -rpc-port=1235 -http-port=8081
noded21:
	go run main.go -id=2 -catching-up=false -rpc-port=1235 -http-port=8081
noded3:
	go run main.go -id=3 -catching-up=true -rpc-port=1236 -http-port=8082
noded31:
	go run main.go -id=3 -catching-up=false -rpc-port=1236 -http-port=8082

nodes1:
	go run main.go -id=1
nodes2:
	go run main.go -id=2
nodes3:
	go run main.go -id=3

clear:
	rm -rf data/

dc:
	docker compose up -d --build

k8s:
	kubectl apply -f k8s-static-cluster.yml
