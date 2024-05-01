package main

import (
	"log"
	"net/rpc"
	"time"
)

type Client struct {
	client *rpc.Client
}

type ClientWriteEntry struct {
	Filename string
	ID       string
	Data     string
}

type ClientWriteReply struct {
	Success bool
	LeaderIP string
}

type ClientReadEntry struct {
	Filename string
	Column   string // either "ID" or "User"
	Value    string
}

type ClientReadReply struct {
	Data    []string
	Success bool
	LeaderIP string
}

// Function to initialize the client by connecting to the leader's RPC server
func NewClient(addr string) (*Client, error) {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{client}, nil
}

// Function to write data to the leader using the ClientWrite RPC
func (c *Client) ClientWrite(entry ClientWriteEntry) error {
	var reply ClientWriteReply
	err := c.client.Call("RaftNode.ClientWrite", entry, &reply)
	if err != nil {
		return err
	}
	return nil
}

// Function to read data from the leader using the ClientRead RPC
func (c *Client) ClientRead(entry ClientReadEntry) (ClientReadReply, error) {
	var reply ClientReadReply
	err := c.client.Call("RaftNode.ClientRead", entry, &reply)
	if err != nil {
		return ClientReadReply{}, err
	}
	return reply, nil
}

func main() {
	// Initialize the client and connect to the leader's RPC server
	client, err := NewClient("localhost:4041") 
	if err != nil {
		log.Fatal("Error connecting to the leader:", err)
	}

	// Example usage: write data
	// writeEntry := ClientWriteEntry{
	// 	Filename: "profile",
	// 	ID: "123",
	// 	Data: "'acronym': 'JD', 'bio': 'Software Engineer', 'email': 'jd@example.com', 'id': '123'",
	// }
	// err = client.ClientWrite(writeEntry)
	// if err != nil {
	// 	log.Fatal("Error writing data:", err)
	// }

	// time.Sleep(5 * time.Second)

	// writeEntry_2 := ClientWriteEntry{
	// 	Filename: "profile",
	// 	ID: "123",
	// 	Data: "'acronym': 'SL', 'bio': 'Student', 'email': 'yl106@wellesley.com', 'id': '2365'",
	// }
	// err = client.ClientWrite(writeEntry_2)
	// if err != nil {
	// 	log.Fatal("Error writing data:", err)
	// }

	writeEntry_message := ClientWriteEntry{
		Filename: "message",
		ID: "133590987",
		Data: "'acronym': 'SL', 'timestamp': '2024/2/1', 'user': 'sandy', 'text': 'hihi'",
	}
	err = client.ClientWrite(writeEntry_message)
	if err != nil {
		log.Fatal("Error writing data:", err)
	}

	time.Sleep(5 * time.Second)

	// Example usage: read data
	readEntry := ClientReadEntry{
		Filename: "message",
		Column:	"user",
		Value: "sandy",
	}
	reply, err := client.ClientRead(readEntry)
	if err != nil {
		log.Fatal("Error reading data:", err)
	}
	log.Println("Read data:", reply.Data)
}