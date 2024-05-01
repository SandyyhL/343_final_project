package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	netRPC "net/rpc"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
)

type SystemConnection struct {
	connection    netRPC.Client
	addressString string
}

var connectionPoint SystemConnection

// 343 project specs
type ClientWriteEntry struct {
	Filename string
	ID       string
	Data     string
}
type ClientWriteReply struct {
	Success  bool
	LeaderID string
}
type ClientReadEntry struct {
	Filename string
	Column   string // either “ID” or “User”
	Value    string
}
type ClientReadReply struct {
	Data     string
	Success  bool
	LeaderID string
}

// Format of arguments / replies expected from JS API call
type SetArgs struct {
	DocName, ItemID, ItemValue string
}

type GetArgs struct {
	DocName, AttributeNAme, ItemID string
}

type Response struct {
	Result string
}

type WriteService struct{}
type ReadService struct{}

func (t *WriteService) SetDocs(r *http.Request, args *SetArgs, result *Response) error {
	// Prepare the variables for 343 RPC call
	theArgs := ClientWriteEntry{
		Filename: args.DocName,
		ID:       args.ItemID,
		Data:     args.ItemValue,
	}
	theReply := ClientWriteReply{}

	// Make the RPC with appropriate arguments
	err := connectionPoint.connection.Call("RaftNode.ClientMakeRequest", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}

	// First check if node was indeed leader
	if theReply.Success {
		*result = Response{Result: "Success"}
		return nil
	}
	// Otherwise, renew connection object
	// Connect to the indicated leader node
	client, err := netRPC.DialHTTP("tcp", theReply.LeaderID)
	if err != nil {
		log.Fatal("Problem with dialing:", err)
	}
	// Save new connection information
	connectionPoint = SystemConnection{*client, theReply.LeaderID}

	// Try again
	// Make the RPC with appropriate arguments
	theReply = ClientWriteReply{}
	err = connectionPoint.connection.Call("RaftNode.ClientMakeRequest", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}
	// First, check if node was indeed leader
	// By now, this should always be true
	if theReply.Success {
		*result = Response{Result: "Success"}
		return nil
	}
	// Otherwise, indicate an error
	return errors.New("Couldn't get to leader")
}

func (t *ReadService) GetDocs(r *http.Request, args *GetArgs, result *Response) error {
	// Prepare the variables for 343 RPC call
	theArgs := ClientReadEntry{
		Filename: args.DocName,
		Column:   args.AttributeNAme,
		Value:    args.ItemID,
	}
	theReply := ClientReadReply{}

	// Make the RPC with appropriate arguments
	err := connectionPoint.connection.Call("RaftNode.ClientMakeRequest", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}

	// First check if node was indeed leader
	if theReply.Success {
		*result = Response{Result: theReply.Data}
		return nil
	}
	// Otherwise, renew connection object
	// Connect to the indicated leader node
	client, err := netRPC.DialHTTP("tcp", theReply.LeaderID)
	if err != nil {
		log.Fatal("Problem with dialing:", err)
	}
	// Save new connection information
	connectionPoint = SystemConnection{*client, theReply.LeaderID}

	// Try again
	// Make the RPC with appropriate arguments
	theReply = ClientReadReply{}
	err = connectionPoint.connection.Call("RaftNode.ClientMakeRequest", theArgs, &theReply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}
	// First, check if node was indeed leader
	// By now, this should always be true
	if theReply.Success {
		*result = Response{Result: theReply.Data}
		return nil
	}
	// Otherwise, indicate an error
	return errors.New("Couldn't get to leader")
}

func connectToNode(fileName string) {
	// --- Read the IP:port info from the cluster configuration file
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Choose a random cluster node to connect to
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	IPAddress := lines[r.Intn(len(lines))]
	log.Println("Chose %s from cluster", IPAddress)

	// Connect to the chosen node
	client, err := netRPC.DialHTTP("tcp", IPAddress)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Save connection information
	connectionPoint = SystemConnection{*client, IPAddress}
}

func main() {
	arguments := os.Args
	// The only value sent should be the cluster file
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Set up a connection to a random node in the cluster
	connectToNode(arguments[1])

	// Start the webservice
	rpcServer := rpc.NewServer()

	rpcServer.RegisterCodec(json.NewCodec(), "application/json")
	rpcServer.RegisterCodec(json.NewCodec(), "application/json;charset=UTF-8")

	write := new(WriteService)
	read := new(ReadService)

	rpcServer.RegisterService(write, "set")
	rpcServer.RegisterService(read, "get")

	router := mux.NewRouter()
	router.Handle("/delivery", rpcServer)
	http.ListenAndServe(":5566", router)
}
