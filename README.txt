How to run code:

1. Open 6 terminals (5 for cluster, 1 for client (cd client))
2. Within each terminal, enter the command but do not hit enter to run it yet:
    a. terminal 1: go run raftNode_final.go 0 cluster.txt
    b. terminal 2: go run raftNode_final.go 1 cluster.txt
    c. terminal 3: go run raftNode_final.go 2 cluster.txt
    d. terminal 4: go run raftNode_final.go 3 cluster.txt
    e. terminal 5: go run raftNode_final.go 4 cluster.txt
    f. terminal 6: go run client.go
3. Start only the nodes (hit enter in the first 5 terminals)
4. Once a leader has been elected and heartbeats are being sent, you may start the client (hit enter in the 6th terminal)