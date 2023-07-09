node1:
	go run main.go -id=0
node2:
	go run main.go -id=1
node3:
	go run main.go -id=2
clear:
	rm -f log.1.dat
	rm -f log.2.dat
	rm -f log.3.dat
