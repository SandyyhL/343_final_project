package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
	"encoding/json"
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
	log        []LogEntry
	matchIndex []int
	nextIndex  []int
	currentLeader int
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

type AppendEntryLeaderReply struct {
	Success bool
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
	Index int
	Term  int
	CommandEntries []CommandEntry
}

type CommandEntry struct {
	LogType string
	Profile_Acronym string
	Profile_Bio string
	Profile_DisplayName string
	Profile_Email string
	Profile_Friends []string
	Profile_ID int
	Profile_Name string
	Profile_Photo string
	Profile_Status string
	Profile_Username string
	Message_ID int
	Message_Timestamp string
	Message_User string
	Message_Text string
	Post_Date string
	Post_Location string
	Post_Rating int
	Post_Description string
	Post_Timestamp string
	Post_Title string
	Post_User string
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

func readProfie(filename string) ([]ProfileEntries, error) {
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var people []ProfileEntries
    decoder := json.NewDecoder(file)
    // Assuming the file contains an array of People
    if err := decoder.Decode(&people); err != nil {
        return nil, err
    }
    return people, nil
}

// Writes all people to a JSON file using Encoder
func writeProfile(people []ProfileEntries, filename string) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    encoder := json.NewEncoder(file)
    return encoder.Encode(people)
}

func (node *RaftNode) AppendEntryLeader(argument CommandEntry, reply *AppendEntryLeaderReply) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.status != "leader" {
		//direct the client to the right leader
		reply.Success = false
		return 
	} else {
		node.log = append(node.log, argument) //append into the leader's log
		//call every follower to append this log
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
				Entries:      []CommandEntry{argument},
				LeaderCommit: raftNode.commitIndex,
			}
	
			go func(node ServerConnection, index int) {
				var reply AppendEntryReply
				err := node.rpcConnection.Call("RaftNode.AppendProfileEntryFollower", args, &reply)
				if err != nil {
					log.Printf("Error sending AppendEntry to node %d: %v", node.serverID, err)
					return
				}
	
				if newEntry {
					if reply.Success {
						repliesReceived++
						if repliesReceived >= majority {
							raftNode.commitIndex++
						}
						raftNode.nextIndex[index]++
						raftNode.matchIndex[index]++
						//leader has to write to its own harddisk
						entryToCommit = raftNode.log[len(raftNode.log)-1]
						fileToWrite = entryToCommit.LogType + ".json"
						profiles, err := readProfile(fileToWrite)
						if err != nil {
							panic(err) // Proper error handling is needed in production code
							log.Println("error in reading file")
						}
						profiles = append(profile, arguments.ProfileEntries)
						if err := writePeople(profiles, fileToWrite); err != nil {
							panic(err) 
							log.Pritnln("error in writing file")
						}
					} else {
						raftNode.nextIndex[index]--
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
		original_index = float64(len(node.log))
		node.commitIndex = int(math.Min(float64(arguments.LeaderCommit), float64(len(node.log))))
		write//TODO
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

	for range ticker.C {
		// Check if the node is still in the leader state
		if node.status != "leader" {
			return
		}

		fmt.Println("Sending heartbeat to followers...")
		go node.appendEntriesToFollowers(false)
	}
}

func (raftNode *RaftNode) appendEntriesToFollowers(newEntry bool) {
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
					}
					raftNode.nextIndex[index]++
					raftNode.matchIndex[index]++

				} else {
					raftNode.nextIndex[index]--
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

func (node *RaftNode) resetElectionTimer() {
	if node.electionTimer != nil {
		node.electionTimer.Stop()
	}
	node.electionTimer = time.AfterFunc(randomElectionTimeout(), node.LeaderElection)
}

func randomElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
}

func (raftNode *RaftNode) ClientAddToLog() {
	for {
		if raftNode.status == "leader" {
			// raftNode.lastApplied = len(raftNode.log)
			entry := LogEntry{len(raftNode.log) + 1, raftNode.currentTerm}
			log.Println("Client communication created the new log entry at index " + strconv.Itoa(entry.Index))
			raftNode.log = append(raftNode.log, entry)
			raftNode.appendEntriesToFollowers(true)
		}
		// HINT 2: force the thread to sleep for a good amount of time (less
		// than that of the leader election timer) and then repeat the actions above.
		// You may use an endless loop here or recursively call the function
		// HINT 3: you don’t need to add to the logic of creating new log
		// entries, just handle the replication
		time.Sleep(3000 * time.Millisecond)
	}
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
	//raftNode.lastApplied = 0
	raftNode.log = make([]LogEntry, 0)

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

	// Start the ClientAddToLog function in a separate thread
	go raftNode.ClientAddToLog()

	select {}
}
