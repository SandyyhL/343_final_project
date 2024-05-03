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
	Column   string // either "id" or "user"
	Value    string
}

type ClientReadReply struct {
	Data    []string
	Success bool
	LeaderIP string
}

func NewClient(addr string) (*Client, error) {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{client}, nil
}

func (c *Client) ClientWrite(entry ClientWriteEntry) error {
    var reply ClientWriteReply
    err := c.client.Call("RaftNode.ClientWrite", entry, &reply)
    if err != nil {
        return err
    }
    // Retry if the request was sent to a follower instead of the leader
    if reply.LeaderIP != "" {
        c.client, err = rpc.DialHTTP("tcp", reply.LeaderIP)
        if err != nil {
            return err
        }
        err = c.ClientWrite(entry)
        if err != nil {
            return err
        }
    }
    return nil
}

func (c *Client) ClientRead(entry ClientReadEntry) (ClientReadReply, error) {
    var reply ClientReadReply
    err := c.client.Call("RaftNode.ClientRead", entry, &reply)
    if err != nil {
        return ClientReadReply{}, err
    }
    // Retry if the request was sent to a follower instead of the leader
    if reply.LeaderIP != "" {
        c.client, err = rpc.DialHTTP("tcp", reply.LeaderIP)
        if err != nil {
            return ClientReadReply{}, err
        }
        reply, err = c.ClientRead(entry)
        if err != nil {
            return ClientReadReply{}, err
        }
    }
    return reply, nil
}

func main() {

	client, err := NewClient("localhost:4041") 
	if err != nil {
		log.Fatal("Error connecting to the leader:", err)
	}

	// write data
	writeEntry := ClientWriteEntry{
		Filename: "profile",
		ID: "123",
		Data: "'acronym': 'JC', 'bio': 'Wellesley Student', 'email': 'jc@wellesley.edu', 'id': '123'",
	}
	err = client.ClientWrite(writeEntry)
	if err != nil {
		log.Fatal("Error writing data:", err)
	}

	time.Sleep(5 * time.Second)

	// read data
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