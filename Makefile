# multiple processes mode
node1:
	# go run -race main.go -id=0
	go run main.go -id=0
node2:
	# go run -race main.go -id=1
	go run main.go -id=1
node3:
	# go run -race main.go -id=2
	go run main.go -id=2
# single process mode
run:
	go run main.go
clear:
	rm -f log.1.dat
	rm -f log.2.dat
	rm -f log.3.dat
