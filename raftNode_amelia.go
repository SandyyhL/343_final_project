// package main

// import (
// 	"bufio"
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"net/http"
// 	"net/rpc"
// 	"os"
// 	"strconv"
// 	"sync"
// 	"time"
// )

// type RaftNode int

// type LogEntry struct {
// 	Index int
// 	Term  int
// }

// type VoteArguments struct {
// 	Term        int
// 	CandidateID int

// 	LastLogIndex int // index of candidate’s last log entry
// 	LastLogTerm  int // term of candidate’s last log entry
// }

// type VoteReply struct {
// 	Term       int
// 	ResultVote bool
// }

// type AppendEntryArgument struct {
// 	Term     int
// 	LeaderID int
// 	PrevLogIndex int
// 	PrevLogTerm  int
// 	Entry        []LogEntry
// 	ProfileEntries []ProfileEntry
// 	MessageEntries []MessageEntry
// 	PostEntries    []PostEntry
// 	LeaderCommit int
// }

// type AppendEntryReply struct {
// 	Term    int
// 	Success bool
// }

// type ServerConnection struct {
// 	serverID      int
// 	Address       string
// 	rpcConnection *rpc.Client
// }

// type ProfileEntry struct {
// 	Acronym string
// 	Bio string
// 	DisplayName string
// 	Email string
// 	Friends []string
// 	ID int
// 	Name string
// 	Photo string
// 	Status string
// 	Username string
// }

// type MessageEntry struct {
// 	ID int
// 	Timestamp string
// 	User string
// 	Text string
// }

// type PostEntry struct {
// 	Date string
// 	Location string
// 	Rating int
// 	Text_description string
// 	Timestamp string
// 	Title string
// 	User string
// }

// var selfID int
// var serverNodes []ServerConnection           // contains ALL nodes in cluster INCLUDING itself
// var currentTerm int                          // question: when all is term updated? ans:when node becomes candidate and follower gets a request vote with higher term
// var votedFor map[int]int = make(map[int]int) // maps terms to votes. track if I voted in a given term and which candidate I voted for

// var mu sync.Mutex
// var election_timer *time.Timer  // for leader election
// var heartbeat_timer *time.Timer // for heartbeats
// var leader int
// var status string              // can be leader, candidate, or follower (follower on startup)
// var heartbeat_interval int = 1 // send heartbeat every 10 ms
// var lastAppliedIndex int       // the last index I applied to my log
// var nextIndex map[int]int      // next index in follower's log to edit
// var myLog []LogEntry           // my log (list of log entries)

// // The RequestVote RPC as defined in Raft
// // Hint 1: Use the description in Figure 2 of the paper
// // Hint 2: Only focus on the details related to leader election and majority votes
// func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
// 	mu.Lock()
// 	defer mu.Unlock()

// 	fmt.Println("Received RequestVote from", arguments.CandidateID, "with args struct.", arguments, "my currentTerm:", currentTerm)
// 	// fmt.Println("RV my log:", myLog)

// 	// if I think I'm leader/candidate but receive a ReqVote from a higher term node, I demote myself
// 	if arguments.Term >= currentTerm && (status == "leader" || status == "candidate") {
// 		status = "follower"
// 		currentTerm = arguments.Term
// 		startElectionTimer()

// 		// return nil // question should I return nil or proceed and cast a vote?
// 	}

// 	if arguments.Term < currentTerm { // ignore nodes from the past
// 		fmt.Println("ignore votes from past terms")
// 		reply.ResultVote = false
// 		reply.Term = currentTerm
// 		return nil
// 	}

// 	_, ok := votedFor[arguments.Term] // help check if I've voted in this term
// 	votedFor_null_or_candidateidalready := !ok || votedFor[arguments.Term] == arguments.CandidateID || votedFor[arguments.Term] == -1

// 	var candidateLog_atLeastUpToDateWithMine bool
// 	if arguments.LastLogTerm != currentTerm {
// 		candidateLog_atLeastUpToDateWithMine = arguments.LastLogTerm >= currentTerm
// 	} else if arguments.LastLogTerm == currentTerm {
// 		candidateLog_atLeastUpToDateWithMine = (arguments.LastLogIndex >= len(myLog)-1)
// 	}
// 	// fmt.Println("RV candidateLog_atLeastUpToDateWithMine:", candidateLog_atLeastUpToDateWithMine)

// 	if votedFor_null_or_candidateidalready && candidateLog_atLeastUpToDateWithMine {
// 		// fmt.Println("Grant yes. I either didn't vote yet or already voted for", arguments.CandidateID, "\nmy before map:", votedFor)
// 		votedFor[arguments.Term] = arguments.CandidateID
// 		reply.ResultVote = true // actually GRANTS yes vote
// 		reply.Term = arguments.Term
// 		currentTerm = arguments.Term
// 		startElectionTimer()
// 	}

// 	return nil
// }

// // function that reads profiles from 
// func readProfiles(filename string) ([]ProfileEntries, error) {
//     file, err := os.Open(filename)
//     if err != nil {
//         return nil, err
//     }
//     defer file.Close()

//     var people []ProfileEntries
//     decoder := json.NewDecoder(file)
//     // Assuming the file contains an array of People
//     if err := decoder.Decode(&people); err != nil {
//         return nil, err
//     }
//     return people, nil
// }

// // Writes all people to a JSON file using Encoder
// func writeProfiles(people []ProfileEntries, filename string) error {
//     file, err := os.Create(filename)
//     if err != nil {
//         return err
//     }
//     defer file.Close()

//     encoder := json.NewEncoder(file)
//     return encoder.Encode(people)
// }

// // The AppendEntry RPC as defined in Raft
// // Hint 1: Use the description in Figure 2 of the paper
// // Hint 2: Only focus on the details related to leader election and heartbeats
// func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
// 	mu.Lock()
// 	defer mu.Unlock()

// 	if arguments.Term >= currentTerm { // I know there is definitely a valid leader
// 		startElectionTimer()
// 		leader = arguments.LeaderID
// 		currentTerm = arguments.Term
// 	}

// 	if arguments.Term < currentTerm { // point 1 in figure 2
// 		reply.Success = false
// 		return nil
// 	}

// 	if arguments.Term >= currentTerm && (status == "leader" || status == "candidate") { // if sender has higher term and I'm currently leader/candidate
// 		status = "follower"          // I demote myself to follower
// 		currentTerm = arguments.Term // tentative for now idk cuz log replication
// 		fmt.Println("in app ent: just demoted myself")
// 		// return nil continue todo here
// 	}

// 	HeartbeatOnly := arguments.Entry == LogEntry{0, 0}
// 	if HeartbeatOnly {
// 		lastAppliedIndex = arguments.LeaderCommit // always stay up to date with leader
// 		currentTerm = arguments.Term
// 		leader = arguments.LeaderID
// 		return nil // otherwise, continue with log logic
// 	}

// 	// fmt.Println("AppEnt: new entry:", arguments.Entry)

// 	if myLog[arguments.PrevLogIndex].Term != arguments.PrevLogTerm { // point 2 in figure 2
// 		// "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm"
// 		reply.Success = false
// 		return nil
// 	}

// 	if len(myLog)-1 >= arguments.Entry.Index && myLog[arguments.Entry.Index].Term != arguments.Entry.Term { // point 3
// 		myLog = myLog[0 : arguments.PrevLogIndex+1] // "delete existing entry and all that follow it"
// 	}

// 	myLog = append(myLog, arguments.Entry) // point 4 in figure 2

// 	if arguments.LeaderCommit > lastAppliedIndex { // point 5
// 		lastAppliedIndex = min(arguments.LeaderCommit, len(myLog)-1) // catch up to leader's committed index
// 	}
// 	reply.Success = true
// 	// fmt.Println("AppEnt my lastAppIndex:", lastAppliedIndex)
// 	return nil
// }

// // You may use this function to help with handling the election time out
// // Hint: It may be helpful to call this method every time the node wants to start an election
// func LeaderElection() {
// 	mu.Lock()
// 	defer mu.Unlock()

// 	// infinite loop in a thread here to check for timer firing?

// 	currentTerm++ // incrememnt own term
// 	fmt.Println("I start election in (new) term", currentTerm)
// 	startElectionTimer()
// 	status = "candidate"  // I become a candidate
// 	numVotesReceived := 1 // I vote for myself

// 	numNodes := len(serverNodes)
// 	numNeededVotes := (numNodes / 2) + 1

// 	victory := make(chan string)

// 	var mu_numVotes sync.Mutex // protect incrementation of vote counting
// 	// send RequestVotes to all other nodes
// 	for _, neighbor := range serverNodes {
// 		if neighbor.serverID != selfID {

// 			voteArgs := VoteArguments{
// 				Term:         currentTerm,
// 				CandidateID:  selfID,
// 				LastLogIndex: len(myLog) - 1,
// 				LastLogTerm:  myLog[len(myLog)-1].Term,
// 			}

// 			voteReply := VoteReply{}

// 			fmt.Println("I send requestvote to", (neighbor.serverID), "with args struct", voteArgs)
// 			go func(server ServerConnection, voteCounter *int) {
// 				err := server.rpcConnection.Call("RaftNode.RequestVote", voteArgs, &voteReply)
// 				if err != nil {
// 					return
// 				}

// 				mu_numVotes.Lock() // ensure only one thread can incrememnt numVotes at a time
// 				if voteReply.ResultVote == true {
// 					*voteCounter += 1
// 				}
// 				if *voteCounter == numNeededVotes {
// 					// I know I won election.
// 					fmt.Println("I won election.")
// 					victory <- "I won"
// 				} else {
// 					//if we haven't received enough votes (keep going)
// 					//if received >numNeededVotes (ignore)
// 				}
// 				mu_numVotes.Unlock()

// 			}(neighbor, &numVotesReceived)

// 		}
// 	}

// 	<-victory // waiting to hear if I won
// 	// if I never get enough votes, I'll wait forever and someone else will time out

// 	fmt.Println("Victory. I got enough yes votes")
// 	leader = selfID
// 	status = "leader"
// 	election_timer.Stop()
// 	for id := range serverNodes {
// 		// "for each server, index of the next log entry to send to that server (initialized to leader last log index + 1) (Reinitialized after election)"
// 		if id != selfID {
// 			nextIndex[id] = len(myLog) - 1 + 1
// 		}
// 	}
// 	// fmt.Println("After win, lastAppIndex:", lastAppliedIndex)
// 	go Heartbeat()
// }

// // You may use this function to help with handling the periodic heartbeats
// // Hint: Use this only if the node is a leader
// func Heartbeat() {
// 	fmt.Println("I'm gonna start sending heartbeats")

// 	// loop_counter := 0 // FOR ZOMBIE LEADER TESTING
// 	for status == "leader" {
// 		// loop_counter += 1  // FOR ZOMBIE LEADER TESTING
// 		// if loop_counter == 10000 { // FOR ZOMBIE LEADER TESTING
// 		// 	fmt.Println("I will sleep for 10 seconds") // FOR ZOMBIE LEADER TESTING
// 		// 	time.Sleep(10 * time.Second)               // FOR ZOMBIE LEADER TESTING
// 		// } // FOR ZOMBIE LEADER TESTING

// 		for _, neighbor := range serverNodes {
// 			if neighbor.serverID != selfID {

// 				appEntArg := AppendEntryArgument{
// 					Term:     currentTerm,
// 					LeaderID: selfID,

// 					LeaderCommit: lastAppliedIndex, // ?
// 				}

// 				appEntReply := AppendEntryReply{}

// 				go func(server ServerConnection) {
// 					err := server.rpcConnection.Call("RaftNode.AppendEntry", appEntArg, &appEntReply)
// 					if err != nil {
// 						return
// 					}
// 				}(neighbor)
// 			}
// 		}

// 		heartbeat_timer = time.NewTimer(time.Millisecond * time.Duration(1))
// 		<-heartbeat_timer.C // only send messages every heartbeat interval
// 	}
// }

// // var cancel_lead_elec_timer = make(chan int) // send 1 over this channel whenever need to reset lead elec timer
// // use this^ channel to prevent channel draining from setting off unnecessary leader elecs

// // startElectionTimer resets the election timeout to a new random duration.
// // This function should be called whenever an event occurs that prevents the need for a new election,
// // such as receiving a heartbeat from the leader or granting a vote to a candidate.
// func startElectionTimer() {
// 	r := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	duration := time.Duration(r.Intn(150)+151) * time.Millisecond
// 	election_timer.Stop()
// 	election_timer.Reset(duration)

// 	go func() {
// 		<-election_timer.C
// 		// if my timer fires, I have to start my own leader election
// 		fmt.Println("I timed out! I'm gonna start leader elec now")
// 		go LeaderElection()
// 	}()
// }




// // This function is designed to emulate a client reaching out to the server.
// // Note that many of the realistic details are removed, for simplicity
// func ClientAddToLog() {
// 	// In a realistic scenario, the client will find the leader node and communicate with it
// 	// In this implementation, we are pretending that the client reached out to the server somehow
// 	// But any new log entries will not be created unless the server / node is a leader
// 	if status == "leader" {
// 		// lastAppliedIndex here is an int variable that is needed by a node
// 		// to store the value of the last index it used in the log
// 		newEntry := LogEntry{lastAppliedIndex, currentTerm}
// 		log.Println("Client communication created the new log entry at index " + strconv.Itoa(newEntry.Index))
// 		// Add rest of logic here
// 		// fmt.Println("ClientAddToLog my log length:", len(myLog))
// 		numConfirmationsReceived := 1
// 		var mu_numVotes sync.Mutex // protect incrementation of confirmation counting

// 		numNodes := len(serverNodes)
// 		numNeededConfirmations := (numNodes / 2) + 1

// 		majority_replicated := make(chan string) // to catch value if I receive majority replicated confirmation

// 		myLog = append(myLog, newEntry)
// 		// fmt.Println("nextIndex:", nextIndex)

// 		for _, neighbor := range serverNodes { // issue AppendEntry RPCs to followers
// 			if neighbor.serverID != selfID {
// 				appEntArg := AppendEntryArgument{
// 					Term:     currentTerm,
// 					LeaderID: selfID,

// 					PrevLogIndex: len(myLog) - 2,
// 					PrevLogTerm:  myLog[len(myLog)-2].Term,
// 					Entry:        newEntry,
// 					LeaderCommit: lastAppliedIndex,
// 				}
// 				appEntReply := AppendEntryReply{}

// 				go func(server ServerConnection, counter *int, followerID *int) {
// 					err := server.rpcConnection.Call("RaftNode.AppendEntry", appEntArg, &appEntReply)
// 					if err != nil {
// 						return
// 					}
// 					mu_numVotes.Lock()

// 					if appEntReply.Success == false {
// 						fmt.Println("something went wrong")
// 						for appEntReply.Success == false {
// 							nextIndex[*followerID] -= 1

// 							entryToFix := myLog[nextIndex[*followerID]]

// 							newAppEntArg := AppendEntryArgument{
// 								Term:     currentTerm,
// 								LeaderID: selfID,

// 								PrevLogIndex: nextIndex[*followerID] - 1,
// 								PrevLogTerm:  myLog[nextIndex[*followerID]-1].Term,
// 								Entry:        entryToFix,
// 								LeaderCommit: lastAppliedIndex,
// 							}
// 							err := server.rpcConnection.Call("RaftNode.AppendEntry", newAppEntArg, &appEntReply)
// 							if err != nil {
// 								return
// 							}
// 						}
// 						for nextIndex[*followerID] != lastAppliedIndex+1 { // plus 1 or not? maybe plus 2??
// 							nextIndex[*followerID] += 1

// 							remedialEntry := myLog[nextIndex[*followerID]]
// 							newAppEntArg := AppendEntryArgument{
// 								Term:     currentTerm,
// 								LeaderID: selfID,

// 								PrevLogIndex: nextIndex[*followerID] - 1,
// 								PrevLogTerm:  myLog[nextIndex[*followerID]-1].Term,
// 								Entry:        remedialEntry,
// 								LeaderCommit: lastAppliedIndex,
// 							}
// 							err := server.rpcConnection.Call("RaftNode.AppendEntry", newAppEntArg, &appEntReply)
// 							if err != nil {
// 								return
// 							}
// 						}
// 					}

// 					if appEntReply.Success == true {
// 						*counter += 1
// 					}
// 					if *counter == numNeededConfirmations {
// 						// I know majority of follwers replicated this entry
// 						majority_replicated <- "confirm"
// 					} else {
// 						//if we haven't received enough confirmations (keep going)
// 						//if received > numNeededConfirmations (ignore)
// 					}
// 					mu_numVotes.Unlock()
// 				}(neighbor, &numConfirmationsReceived, &neighbor.serverID)
// 			}
// 		}
// 		<-majority_replicated // wait for majority confirmation
// 		lastAppliedIndex += 1 // apply to my own state machine
// 		// fmt.Println("lastAppliedIndex:", lastAppliedIndex)
// 		// HINT 1: using the AppendEntry RPC might happen here

// 	}
// 	// HINT 2: force the thread to sleep for a good amount of time (less
// 	// than that of the leader election timer) and then repeat the actions above.
// 	// You may use an endless loop here or recursively call the function
// 	// HINT 3: you don’t need to add to the logic of creating new log
// 	// entries, just handle the replication
// }

// func main() {
// 	// The assumption here is that the command line arguments will contain:
// 	// This server's ID (zero-based), location and name of the cluster configuration file
// 	arguments := os.Args
// 	if len(arguments) == 1 {
// 		fmt.Println("Please provide cluster information.")
// 		return
// 	}

// 	// Read the values sent in the command line

// 	// Get this sever's ID (same as its index for simplicity)
// 	myID, err := strconv.Atoi(arguments[1])
// 	// Get the information of the cluster configuration file containing information on other servers
// 	file, err := os.Open(arguments[2])
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	myPort := "localhost"

// 	votedFor = make(map[int]int)
// 	votedFor[0] = -1
// 	currentTerm = 0
// 	selfID = myID
// 	leader = -1
// 	status = "follower"
// 	lastAppliedIndex = 1
// 	nextIndex = make(map[int]int)
// 	myLog = make([]LogEntry, 0)
// 	dummyLogEntry := LogEntry{0, 0}
// 	myLog = append(myLog, dummyLogEntry)
// 	election_timer = time.NewTimer(1 * time.Millisecond) // initialize timer so it's never nil
// 	<-election_timer.C                                   // drain it once so doesn't interfere with actual system

// 	// Read the IP:port info from the cluster configuration file
// 	scanner := bufio.NewScanner(file)
// 	lines := make([]string, 0)
// 	index := 0
// 	for scanner.Scan() {
// 		// Get server IP:port
// 		text := scanner.Text()
// 		log.Printf(text, index)
// 		if index == myID {
// 			myPort = text
// 			index++
// 			// continue
// 		}
// 		// Save that information as a string for now
// 		lines = append(lines, text)
// 		index++
// 	}
// 	// If anything wrong happens with readin the file, simply exit
// 	if err := scanner.Err(); err != nil {
// 		log.Fatal(err)
// 	}

// 	// Following lines are to register the RPCs of this object of type RaftNode
// 	api := new(RaftNode)
// 	err = rpc.Register(api)
// 	if err != nil {
// 		log.Fatal("error registering the RPCs", err)
// 	}
// 	rpc.HandleHTTP()
// 	go http.ListenAndServe(myPort, nil)
// 	log.Printf("serving rpc on port" + myPort)

// 	// This is a workaround to slow things down until all servers are up and running
// 	// Idea: wait for user input to indicate that all servers are ready for connections
// 	// Pros: Guaranteed that all other servers are already alive
// 	// Cons: Non-realistic work around

// 	// reader := bufio.NewReader(os.Stdin)
// 	// fmt.Print("Type anything when ready to connect >> ")
// 	// text, _ := reader.ReadString('\n')
// 	// fmt.Println(text)

// 	// Idea 2: keep trying to connect to other servers even if failure is encountered
// 	// For fault tolerance, each node will continuously try to connect to other nodes
// 	// This loop will stop when all servers are connected
// 	// Pro: Realistic setup
// 	// Con: If one server is not set up correctly, the rest of the system will halt

// 	for index, element := range lines {
// 		// Attemp to connect to the other server node
// 		client, err := rpc.DialHTTP("tcp", element)
// 		// If connection is not established
// 		for err != nil {
// 			// Record it in log
// 			log.Println("Trying again. Connection error: ", err)
// 			// Try again!
// 			client, err = rpc.DialHTTP("tcp", element)
// 		}
// 		// Once connection is finally established
// 		// Save that connection information in the servers list
// 		serverNodes = append(serverNodes, ServerConnection{index, element, client})
// 		// Record that in log
// 		fmt.Println("Connected to " + element)
// 	}

// 	// Once all the connections are established, we can start the typical operations within Raft
// 	// Leader election and heartbeats are concurrent and non-stop in Raft

// 	// HINT 1: You may need to start a thread here (or more, based on your logic)
// 	// Hint 2: Main process should never stop
// 	// Hint 3: After this point, the threads should take over
// 	// Heads up: they never will be done!
// 	// Hint 4: wg.Wait() might be helpful here

// 	var wg sync.WaitGroup
// 	wg.Add(1)

// 	fmt.Println("my ID:", selfID)
// 	go startElectionTimer() // for general testing/normal case

// 	// if selfID == 2 { // FOR ZOMBIE LEADER TESTING
// 	// 	status = "leader"
// 	// 	go Heartbeat()
// 	// }
// 	// time.Sleep(5 * time.Second)
// 	// fmt.Println("I think leader is", leader) // FOR ZOMBIE LEADER TESTING

// 	// if selfID == leader { // simulate node 0 dying then reviving
// 	// 	time.Sleep(10 * time.Second)
// 	// }

// 	time.Sleep(3 * time.Second) // wait for leader election
// 	ClientAddToLog()
// 	ClientAddToLog()
// 	time.Sleep(20 * time.Second) // give ourselves 20 seconds to kill the first leader
// 	ClientAddToLog()
// 	time.Sleep(10 * time.Second) // give ClientAddToLog time to finish
// 	fmt.Println("my log:", myLog)

// 	wg.Wait() // Waits forever, so main process does not stop

// }
