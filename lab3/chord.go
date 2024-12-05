package main

import (
	"chord/argparser"
	"errors"
	"log"
	"strconv"
)

const m = 6

type NodeAddress struct {
	IP   string
	Port string
	Id   int
}

type Node struct {
	Next        int // what finger to fix next
	FingerTable map[int]NodeAddress
	Id          int // 6edc84ffbb1c9c250094d78383dd5bf71c5c7a02 -> 12318923719284719 % 2^m -> 43
	Successor   NodeAddress
	Predecessor NodeAddress
	IP          string
	Port        string
	M           int
}

func createNode(c argparser.Config) Node {
	node := Node{
		Next:        0,
		FingerTable: make(map[int]NodeAddress),
		Id:          getIdFromHash(c.Id, m),
		Successor:   NodeAddress{IP: c.Address, Port: strconv.Itoa(c.Port), Id: getIdFromHash(c.Id, m)},
		Predecessor: NodeAddress{},
		IP:          c.Address,
		Port:        strconv.Itoa(c.Port),
		M:           m,
	}

	// init finger table
	for i := 1; i <= node.M; i++ {
		node.FingerTable[i] = node.createAddress()
	}
	return node
}

func joinNode(c argparser.Config) Node {
	n := createNode(c)
	np := NodeAddress{IP: c.JoinAddress, Port: strconv.Itoa(c.JoinPort)}
	log.Printf("Joining node: %+v\n", n)
	reply, err := callFindSuccessor(np, &n.Id)
	if err != nil {
		log.Fatal("failed to join ring:", err)
	}
	n.Successor = *reply

	for i := 1; i <= n.M; i++ {
		log.Printf("Fixing Finger %d:", i)
		n.fixFingers()
	}
	log.Printf("Joined node: %+v\n", n)
	return n
}

func (n *Node) FindSuccessor(id *int, reply *NodeAddress) error {
	log.Printf("Finding successor for id: %d\n", *id)
	if *id > n.Id && *id <= n.Successor.Id {
		reply.IP = n.Successor.IP
		reply.Port = n.Successor.Port
		reply.Id = n.Successor.Id
		log.Printf("Successor found: %+v\n", reply)
		return nil
	}
	next := n.closestPrecedingNode(*id)

	// If the closest preceding node is the current node, then the current node is the successor.
	if next == n.createAddress() {
		reply.IP = n.IP
		reply.Port = n.Port
		reply.Id = n.Id
		log.Printf("Successor found itself: %+v\n", reply)
		return nil
	}

	// TODO should handle error?
	nextReply, _ := callFindSuccessor(next, id)
	reply.IP = nextReply.IP
	reply.Port = nextReply.Port
	reply.Id = nextReply.Id
	return nil
}

func (n *Node) fixFingers() {
	next := n.Next + 1
	if next > m {
		next = 1
	}
	n.Next = next
	id := n.Id + 2<<(next-1)%(2<<n.M)
	reply := new(NodeAddress)
	// TODO should handle error?
	n.FindSuccessor(&id, reply)
	log.Printf("Received reply: %+v\n", reply)
	n.FingerTable[next] = *reply
}

func (n *Node) closestPrecedingNode(id int) NodeAddress {
	for i := n.M; i >= 1; i-- {
		fid := n.FingerTable[i].Id
		if fid > n.Id && fid < id {
			return n.FingerTable[i]
		}
	}
	return n.createAddress()
}

func (n *Node) createAddress() NodeAddress {
	return NodeAddress{
		IP:   n.IP,
		Port: n.Port,
		Id:   n.Id,
	}
}

func callFindSuccessor(node NodeAddress, id *int) (*NodeAddress, error) {
	nodeAdressReply := new(NodeAddress)
	ok := Call("Node.FindSuccessor", node.IP, node.Port, id, nodeAdressReply)
	if !ok {
		return nil, errors.New("failed to call FindSuccessor")
	}
	return nodeAdressReply, nil
}
