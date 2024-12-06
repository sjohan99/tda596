package main

import (
	"chord/argparser"
	"errors"
	"log"
	"strconv"
	"time"
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
	Successors  []NodeAddress
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
		Successors:  make([]NodeAddress, c.Successors),
		Predecessor: NodeAddress{},
		IP:          c.Address,
		Port:        strconv.Itoa(c.Port),
		M:           m,
	}

	for i := 0; i < c.Successors; i++ {
		node.Successors[i] = NodeAddress{IP: c.Address, Port: strconv.Itoa(c.Port), Id: getIdFromHash(c.Id, m)}
	}

	// init finger table
	for i := 1; i <= node.M; i++ {
		node.FingerTable[i] = node.createAddress()
	}
	return node
}

func joinNode(c argparser.Config) Node {
	n := createNode(c)
	np := NodeAddress{IP: c.JoinAddress, Port: strconv.Itoa(c.JoinPort), Id: getIdFromHash(c.JoinId, m)}
	log.Printf("Joining node: %+v\n", n)
	reply, err := callGetSuccessorList(np)
	if err != nil {
		log.Fatal("failed to join ring:", err)
	}
	successors := *reply // [1,2,3,4,5]
	successors = append([]NodeAddress{np}, successors[:len(successors)-1]...)
	n.Successors = successors

	for i := 1; i <= n.M; i++ {
		log.Printf("Fixing Finger %d:", i)
		n.fixFingers()
	}
	log.Printf("Joined node: %+v\n", n)
	return n
}

func (n *Node) Start(c argparser.Config) {
	n.StartServer(n.IP, n.Port)
	go func() {
		for {
			time.Sleep(c.StabilizeInterval)
			n.stabilize()
		}
	}()
	go func() {
		for {
			time.Sleep(c.FixFingersInterval)
			n.fixFingers()
		}
	}()
	go func() {
		for {
			time.Sleep(c.CheckPredecessorInterval)
			n.checkPredecessor()
		}
	}()
}

func (n *Node) HealthCheck(_ *struct{}, _ *struct{}) error {
	return nil
}

func (n *Node) GetSuccessorList(_ *struct{}, reply *[]NodeAddress) error {
	*reply = n.Successors
	return nil
}

func (n *Node) Notify(node *NodeAddress, _ *struct{}) error {
	if predecessorNotNil(n.Predecessor) || (node.Id > n.Predecessor.Id && node.Id < n.Id) {
		n.Predecessor = *node
	}
	return nil
}

func (n *Node) GetPredecessor(_ *struct{}, reply *NodeAddress) error {
	*reply = n.Predecessor
	return nil
}

func (n *Node) FindSuccessor(id *int, reply *NodeAddress) error {
	log.Printf("Finding successor for id: %d\n", *id)
	for _, succ := range n.Successors {
		if *id > n.Id && *id <= succ.Id {
			reply.IP = succ.IP
			reply.Port = succ.Port
			reply.Id = succ.Id
			log.Printf("Successor found: %+v\n", reply)
			return nil
		}
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
	// Check finger table
	for i := n.M; i >= 1; i-- {
		fid := n.FingerTable[i].Id
		// TODO is fid < id correct?
		if fid > n.Id && fid < id {
			return n.FingerTable[i]
		}
	}

	closest := n.FingerTable[n.M]
	// Check successors
	for _, succ := range n.Successors {
		if succ.Id > closest.Id && succ.Id < id {
			closest = succ
		}
	}

	// TODO check correctness
	if closest == n.FingerTable[n.M] {
		return n.createAddress()
	}

	return closest
}

func (n *Node) stabilize() {
	for _, succ := range n.Successors {
		successorList, err := callGetSuccessorList(succ)
		if err != nil {
			log.Printf("failed to stabilize with successor: %+v\n", succ)
			continue
		}
		predecessor, err := callGetPredecessor(succ)
		if err != nil {
			log.Printf("failed to stabilize with successor: %+v\n", succ)
			continue
		}

		newSuccessors := []NodeAddress{}

		if predecessorNotNil(*predecessor) { // is predecessor between n and succ?
			if predecessor.Id > n.Id && predecessor.Id < succ.Id {
				newSuccessors = append(newSuccessors, *predecessor)
			} else if succ.Id == n.Id {

				newSuccessors = append(newSuccessors, *predecessor)
			}
		}

		successors := *successorList
		newSuccessors = append(newSuccessors, succ)
		newSuccessors = append(newSuccessors, successors[:len(successors)-len(newSuccessors)]...)
		n.Successors = newSuccessors

		callNotify(n.Successors[0], n.createAddress())
		return
	}
	log.Fatalf("Shutting down. Could not stabilize with any successor: %+v\n", n.Successors)
}

func (n *Node) checkPredecessor() {
	if predecessorNotNil(n.Predecessor) && !callHealthCheck(n.Predecessor) {
		n.Predecessor = NodeAddress{}
	}
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

func callGetSuccessorList(node NodeAddress) (*[]NodeAddress, error) {
	successorsReply := new([]NodeAddress)
	ok := Call("Node.GetSuccessorList", node.IP, node.Port, new(struct{}), successorsReply)
	if !ok {
		return nil, errors.New("failed to call GetSuccessorList")
	}
	return successorsReply, nil
}

func callGetPredecessor(node NodeAddress) (*NodeAddress, error) {
	predecessorReply := new(NodeAddress)
	ok := Call("Node.GetPredecessor", node.IP, node.Port, new(struct{}), predecessorReply)
	if !ok {
		return nil, errors.New("failed to call GetPredecessor")
	}
	return predecessorReply, nil
}

func callHealthCheck(node NodeAddress) bool {
	return Call("Node.HealthCheck", node.IP, node.Port, new(struct{}), new(struct{}))
}

func callNotify(node NodeAddress, n NodeAddress) {
	Call("Node.Notify", node.IP, node.Port, n, new(struct{}))
}
