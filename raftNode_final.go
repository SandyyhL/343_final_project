package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RaftNode struct {
	mu            sync.Mutex
	selfID        int
	myPort        string
	electionTimer *time.Timer
	status        string
	currentTerm   int
	serverNodes   []ServerConnection
	votedFor      int
	commitIndex   int
	lastApplied   int
	log           []LogEntry
	matchIndex    []int
	nextIndex     []int
	folder        string
}

type VoteArguments struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntryArgument struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

type LogEntry struct {
	Index   int
	Term    int
	Entries []ClientWriteEntry
}

type ClientWriteEntry struct {
	Filename string
	ID       string
	Data     string
}

type ClientWriteReply struct {
	Success bool
}

type ClientReadEntry struct {
	Filename string
	Column   string // either "ID" or "User"
	Value    string
}

type ClientReadReply struct {
	Data    []string
	Success bool
}

const (
	// ElectionTimeoutMin = 300
	// ElectionTimeoutMax = 600
	// HeartbeatInterval  = 200
	ElectionTimeoutMin = 3000
	ElectionTimeoutMax = 6000
	HeartbeatInterval  = 2000
)

func (node *RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if arguments.Term > node.currentTerm {
		node.currentTerm = arguments.Term
	}

	reply.Term = node.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if arguments.Term < node.currentTerm {
		reply.VoteGranted = false
		return nil
	}

	if (node.votedFor == -1 || node.votedFor == arguments.CandidateID) && (len(node.log) == 0) {
		// Grant the vote since the candidate's log is also empty
		reply.VoteGranted = true
		node.votedFor = arguments.CandidateID
		node.resetElectionTimer()
		return nil
	} else if (node.votedFor == -1 || node.votedFor == arguments.CandidateID) &&
		(arguments.LastLogTerm >= node.log[len(node.log)-1].Term) &&
		(arguments.LastLogIndex >= node.log[len(node.log)-1].Index) {
		// 2. If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		reply.VoteGranted = true
		node.votedFor = arguments.CandidateID
		node.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}

	return nil
}

func (node *RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	log.Printf("%s %d received heartbeat from %x", node.status, node.selfID, arguments.LeaderID)

	reply.Term = node.currentTerm

	// 1. Reply false if term < currentTerm (§5.1)
	if arguments.Term < node.currentTerm {
		reply.Success = false
		return nil
	}

	node.currentTerm = arguments.Term

	if len(node.log) == 0 {
		if arguments.PrevLogIndex == 0 {
			// If log is empty and PrevLogIndex is also 0, the entry can be appended directly
			if len(arguments.Entries) > 0 {
				node.log = append(node.log, arguments.Entries...)
				log.Println("Added new log entry with index " + strconv.Itoa(node.log[len(node.log)-1].Index))
			}
			reply.Success = true
		} else {
			// If log is empty and PrevLogIndex is non-zero, there's an inconsistency
			reply.Success = false
		}
	} else {
		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if len(node.log) < arguments.PrevLogIndex || node.log[arguments.PrevLogIndex-1].Term != arguments.PrevLogTerm {
			reply.Success = false
		}
		// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		if len(node.log) >= arguments.PrevLogIndex && node.log[arguments.PrevLogIndex-1].Term != arguments.PrevLogTerm {
			node.log = node.log[:arguments.PrevLogIndex-1]
			reply.Success = false
		} else {
			reply.Success = true
			// 4. Append any new entries not already in the log
			if len(arguments.Entries) > 0 {
				node.log = append(node.log, arguments.Entries...)
				log.Println("Added new log entry with index " + strconv.Itoa(node.log[len(node.log)-1].Index))
			}
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if arguments.LeaderCommit > node.commitIndex {
		node.commitIndex = int(math.Min(float64(arguments.LeaderCommit), float64(len(node.log))))
	}

	if node.commitIndex > node.lastApplied {
		data := node.log[node.lastApplied].Entries[0]
		node.appendToJSONFile(data)
		node.lastApplied++
	}

	// reset timer if heartbeat
	if len(arguments.Entries) == 0 {
		node.resetElectionTimer()
	}

	if node.votedFor != -1 {
		node.votedFor = -1
	}
	if node.status != "follower" {
		node.status = "follower"
	}

	return nil
}

func (raftNode *RaftNode) LeaderElection() {
	raftNode.mu.Lock()
	raftNode.currentTerm++
	raftNode.votedFor = raftNode.selfID
	raftNode.status = "candidate"
	raftNode.resetElectionTimer()
	raftNode.mu.Unlock()

	var votesReceived = 0
	totalServers := len(raftNode.serverNodes)
	majority := totalServers/2 + 1

	args := VoteArguments{
		Term:        raftNode.currentTerm,
		CandidateID: raftNode.selfID,
	}

	// Check if the log is empty
	if len(raftNode.log) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = raftNode.log[len(raftNode.log)-1].Index
		args.LastLogTerm = raftNode.log[len(raftNode.log)-1].Term
	}

	fmt.Printf("Node %d is now %s\n", raftNode.selfID, raftNode.status)

	for _, node := range raftNode.serverNodes {
		go func(node ServerConnection) {
			var reply VoteReply
			err := node.rpcConnection.Call("RaftNode.RequestVote", args, &reply)
			if err != nil {
				log.Printf("Error sending RequestVote to node %d: %v", node.serverID, err)
				return
			}
			raftNode.mu.Lock()
			defer raftNode.mu.Unlock()
			if reply.Term > raftNode.currentTerm {
				raftNode.currentTerm = reply.Term
				return
			}
			if reply.VoteGranted {
				votesReceived++
				if votesReceived >= majority && raftNode.status != "leader" {
					raftNode.status = "leader"
					raftNode.electionTimer.Stop()
					fmt.Printf("Node %d becomes %s for term %d\n", raftNode.selfID, raftNode.status, raftNode.currentTerm)
					raftNode.initNextIndex()
					raftNode.initMatchIndex()
					go raftNode.Heartbeat()
					return
				}
			}
		}(node)
	}
}

func (node *RaftNode) Heartbeat() {
	ticker := time.NewTicker(HeartbeatInterval * time.Millisecond)
	defer ticker.Stop()
	var emptyEntry ClientWriteEntry
	for range ticker.C {
		// Check if the node is still in the leader state
		if node.status != "leader" {
			return
		}

		fmt.Println("Sending heartbeat to followers...")

		go node.appendEntriesToFollowers(false, emptyEntry)
	}
}

func (raftNode *RaftNode) appendEntriesToFollowers(newEntry bool, data ClientWriteEntry) {
	raftNode.mu.Lock()
	defer raftNode.mu.Unlock()

	totalServers := len(raftNode.serverNodes)
	majority := totalServers/2 + 1
	var repliesReceived = 0
	var entries []LogEntry
	var prevLogIndex int
	var prevLogTerm int
	var index int

	for i, node := range raftNode.serverNodes {

		index = raftNode.nextIndex[i]
		if index != 1 {
			prevLogIndex = raftNode.log[index-2].Index
			prevLogTerm = raftNode.log[index-2].Term
		} else {
			prevLogIndex = 0
			prevLogTerm = 0
		}
		if newEntry {
			entry := raftNode.log[index-1]
			entries = []LogEntry{entry}
		}

		args := AppendEntryArgument{
			Term:         raftNode.currentTerm,
			LeaderID:     raftNode.selfID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: raftNode.commitIndex,
		}

		go func(node ServerConnection, index int) {
			var reply AppendEntryReply
			err := node.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
			if err != nil {
				log.Printf("Error sending AppendEntry to node %d: %v", node.serverID, err)
				return
			}

			if newEntry {
				if reply.Success {
					repliesReceived++
					if repliesReceived >= majority {
						raftNode.commitIndex++
						raftNode.lastApplied++
						err := raftNode.appendToJSONFile(data)
						if err != nil {
							print(err)
						}
					}
					raftNode.nextIndex[index]++
					raftNode.matchIndex[index]++

				} else {
					raftNode.nextIndex[index]--
					raftNode.resendLogEntryToFollowers(index)
				}
			}
			if reply.Term > raftNode.currentTerm {
				raftNode.currentTerm = reply.Term
				raftNode.votedFor = -1
				raftNode.status = "follower"
				fmt.Println("Reverting to follower...")
				raftNode.resetElectionTimer()
			}
		}(node, i)
	}
}

func (raftNode *RaftNode) resendLogEntryToFollowers(nodeIndex int) {
	raftNode.mu.Lock()
	defer raftNode.mu.Unlock()

	var entries []LogEntry
	var prevLogIndex int
	var prevLogTerm int
	var index int

	node := raftNode.serverNodes[nodeIndex]

	index = raftNode.nextIndex[nodeIndex]
	if index != 1 {
		prevLogIndex = raftNode.log[index-2].Index
		prevLogTerm = raftNode.log[index-2].Term
	} else {
		prevLogIndex = 0
		prevLogTerm = 0
	}

	entry := raftNode.log[index-1]
	entries = []LogEntry{entry}

	args := AppendEntryArgument{
		Term:         raftNode.currentTerm,
		LeaderID:     raftNode.selfID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: raftNode.commitIndex,
	}

	var reply AppendEntryReply
	err := node.rpcConnection.Call("RaftNode.AppendEntry", args, &reply)
	if err != nil {
		log.Printf("Error sending AppendEntry to node %d: %v", node.serverID, err)
		return
	}

	if reply.Success {
		raftNode.nextIndex[nodeIndex]++
		raftNode.matchIndex[nodeIndex]++
		return
	} else {
		raftNode.nextIndex[nodeIndex]--
		raftNode.resendLogEntryToFollowers(nodeIndex)
	}
}

func (node *RaftNode) resetElectionTimer() {
	if node.electionTimer != nil {
		node.electionTimer.Stop()
	}
	node.electionTimer = time.AfterFunc(randomElectionTimeout(), node.LeaderElection)
}

func randomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
}

func (raftNode *RaftNode) ClientWrite(data ClientWriteEntry, reply *ClientWriteReply) error {
	raftNode.mu.Lock()
	defer raftNode.mu.Unlock()

	if raftNode.status != "leader" {
		// If this node is not the leader, reject the client write request
		reply.Success = false
		return nil
	}

	// Append the client's data to the log and replicate it to followers
	entry := LogEntry{
		Index:   len(raftNode.log) + 1,
		Term:    raftNode.currentTerm,
		Entries: []ClientWriteEntry{data},
	}

	raftNode.log = append(raftNode.log, entry)
	err := raftNode.appendToJSONFile(data)
	if err != nil {
		log.Printf("Error: ", err)
	}
	go raftNode.appendEntriesToFollowers(true, data)

	reply.Success = true
	return nil
}

func (raftNode *RaftNode) ClientRead(request ClientReadEntry, reply *ClientReadReply) error {
	raftNode.mu.Lock()
	defer raftNode.mu.Unlock()

	if raftNode.status != "leader" {
		reply.Success = false
		return nil
	}

	fileTypeFilename := request.Filename 
	allEntries, err := raftNode.readFromJSONFile(fileTypeFilename) // Assume returns ([]string, error)
	if err != nil {
		reply.Success = false
		return err
	}

	if request.Value == "all" {
		reply.Data = allEntries
		reply.Success = true
		return nil
	}

	searchCriteria := fmt.Sprintf("%s: %s", request.Column, request.Value)

	for _, entry := range allEntries {
		if strings.Contains(entry, searchCriteria) {
			reply.Data = append(reply.Data, entry)
		}
	}

	// Set success based on whether any entries were found
	reply.Success = len(reply.Data) > 0

	return nil
}

//	creates a file of the following type: {
//	    "456": {
//	        "acronym": "",
//	        "bio": "",
//	        "email": "",
//	        "id": ""
//	    },
//	    "123": {
//	        "acronym": "",
//	        "bio": "",
//	        "email": "",
//	        "id": ""
//	    }
//	}
func (raftNode *RaftNode) appendToJSONFile(entry ClientWriteEntry) error {

	filename := raftNode.folder + entry.Filename  + ".json"

	data := make(map[string]string)
	err := json.Unmarshal([]byte(entry.Data), &data)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a new map
	newData := map[string]map[string]string{
		entry.ID: data,
	}

	fileData, err := json.MarshalIndent(newData, "", "    ")
	if err != nil {
		return err
	}

	// Append the new data to the file
	_, err = file.Write(fileData)
	if err != nil {
		return err
	}

	return nil
}

func (raftNode *RaftNode) readFromJSONFile(fileN string) ([]string, error) {
	filename := raftNode.folder + fileN + ".json"

	existingData := make([]string, 0)
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, err // Return the error if file reading fails
	}

	err = json.Unmarshal(file, &existingData)
	if err != nil {
		return nil, err // Return the error if JSON unmarshaling fails
	}

	return existingData, nil
}

func (raftNode *RaftNode) initMatchIndex() {
	raftNode.matchIndex = make([]int, len(raftNode.serverNodes))
	for i := range raftNode.matchIndex {
		raftNode.matchIndex[i] = 0
	}
}

func (raftNode *RaftNode) initNextIndex() {
	raftNode.nextIndex = make([]int, len(raftNode.serverNodes))
	for i := range raftNode.nextIndex {
		raftNode.nextIndex[i] = len(raftNode.log) + 1
	}
}

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	raftNode := &RaftNode{
		mu:     sync.Mutex{},
		status: "follower",
	}

	raftNode.currentTerm = 0
	raftNode.votedFor = -1
	raftNode.selfID, _ = strconv.Atoi(arguments[1])
	raftNode.commitIndex = 0
	raftNode.lastApplied = 0
	raftNode.log = make([]LogEntry, 0)
	raftNode.folder = "folder" + strconv.Itoa(raftNode.selfID) + "/"

	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	raftNode.myPort = "localhost"

	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		text := scanner.Text()
		if index == raftNode.selfID {
			raftNode.myPort = text
		}
		lines = append(lines, text)
		index++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	err = rpc.Register(raftNode)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(raftNode.myPort, nil)
	log.Printf("serving rpc on port " + raftNode.myPort)

	time.Sleep(5 * time.Second)

	for index, element := range lines {
		if index == raftNode.selfID {
			continue
		}
		client, err := rpc.DialHTTP("tcp", element)
		if err != nil {
			fmt.Println("Error connecting to " + element)
		}
		raftNode.serverNodes = append(raftNode.serverNodes, ServerConnection{index, element, client})
		fmt.Println("Connected to " + element)
	}

	raftNode.resetElectionTimer()

	select {}
}
