package main

import (
	"chord/argparser"
	"context"
	"errors"
	"log"
	"slices"
	"strconv"
	"sync"
	"time"
)

type NodeAddress struct {
	IP   string
	Port string
	Id   int
}

type Node struct {
	mu              sync.Mutex
	Next            int // what finger to fix next
	FingerTable     map[int]NodeAddress
	Id              int // 6edc84ffbb1c9c250094d78383dd5bf71c5c7a02 -> 12318923719284719 % 2^m -> 43
	Successors      []NodeAddress
	Predecessor     NodeAddress
	IP              string
	Port            string
	M               int
	CalculateIdFunc func(string, int) int
}

func CreateNode(c argparser.Config) *Node {
	node := Node{
		Next:        0,
		FingerTable: make(map[int]NodeAddress),
		Id:          c.CalculateIdFunc(c.Id, c.M),
		Successors:  make([]NodeAddress, c.Successors),
		Predecessor: NodeAddress{},
		IP:          c.Address,
		Port:        strconv.Itoa(c.Port),
		M:           c.M,
	}

	for i := 0; i < c.Successors; i++ {
		node.Successors[i] = NodeAddress{IP: c.Address, Port: strconv.Itoa(c.Port), Id: c.CalculateIdFunc(c.Id, c.M)}
	}

	// init finger table
	for i := 1; i <= node.M; i++ {
		node.FingerTable[i] = node.createAddress()
	}
	return &node
}

func JoinNode(c argparser.Config) *Node {
	n := CreateNode(c)
	np := NodeAddress{IP: c.JoinAddress, Port: strconv.Itoa(c.JoinPort), Id: c.CalculateIdFunc(c.JoinId, c.M)}

	reply, err := callFindSuccessor(np, &n.Id)
	if err != nil {
		log.Fatal("failed to join ring:", err)
	}
	for i := range n.Successors {
		n.Successors[i] = *reply
	}

	for i := 1; i <= n.M; i++ {
		n.fixFingers()
	}
	return n
}

func (n *Node) Start(c argparser.Config, ctx *context.Context) {
	if ctx == nil {
		newCtx := context.Background()
		ctx = &newCtx
	}
	n.StartServer(n.IP, n.Port, ctx)
	n.startBackgroundTask(ctx, c.StabilizeInterval, n.stabilize)
	n.startBackgroundTask(ctx, c.FixFingersInterval, n.fixFingers)
	n.startBackgroundTask(ctx, c.CheckPredecessorInterval, n.checkPredecessor)
}

func (n *Node) startBackgroundTask(ctx *context.Context, interval time.Duration, task func()) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				task()
			case <-(*ctx).Done():
				log.Printf("Shutting down background tasks")
				return
			}
		}
	}()
}

func (n *Node) HealthCheck(_ *struct{}, _ *struct{}) error {
	return nil
}

func (n *Node) GetSuccessorList(_ *struct{}, reply *[]NodeAddress) error {
	n.mu.Lock()
	*reply = n.Successors
	n.mu.Unlock()
	return nil
}

func (n *Node) Notify(potentialPredecessor *NodeAddress, _ *struct{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.shouldChangePredecessor(*potentialPredecessor) {
		n.Predecessor = *potentialPredecessor
	}
	return nil
}

func (n *Node) GetPredecessor(_ *struct{}, reply *NodeAddress) error {
	*reply = n.Predecessor
	return nil
}

func (n *Node) copyNodeState() Node {
	n.mu.Lock()
	defer n.mu.Unlock()
	ft := make(map[int]NodeAddress)
	for i, finger := range n.FingerTable {
		ft[i] = finger
	}
	succs := make([]NodeAddress, len(n.Successors))
	copy(succs, n.Successors)
	return Node{
		Next:        n.Next,
		FingerTable: ft,
		Id:          n.Id,
		Successors:  succs,
		Predecessor: n.Predecessor,
		IP:          n.IP,
		Port:        n.Port,
		M:           n.M,
	}
}

func fillReply(reply *NodeAddress, node NodeAddress) {
	reply.IP = node.IP
	reply.Port = node.Port
	reply.Id = node.Id
}

func (n *Node) FindSuccessor(id *int, reply *NodeAddress) error {
	// Copy the nodes state and perform the search on the copy
	// to avoid locking the node for the entire search.
	nCopy := n.copyNodeState()

	for _, succ := range nCopy.Successors {
		if CounterClockwiseDistance(*id, nCopy.Id, nCopy.M) <= CounterClockwiseDistance(succ.Id, nCopy.Id, nCopy.M) {
			fillReply(reply, succ)
			return nil
		}
	}

	closestPrecedingNodes := nCopy.closestPrecedingNodes(*id)

	self := nCopy.createAddress()
	for _, next := range closestPrecedingNodes {
		if next == self {
			fillReply(reply, self)
			return nil
		}
		nextReply, err := callFindSuccessor(next, id)
		if err != nil {
			continue
		}
		fillReply(reply, *nextReply)
		return nil
	}
	return errors.New("failed to find successor")
}

func (n *Node) fixFingers() {
	n.mu.Lock()
	next := n.Next + 1
	if next > n.M {
		next = 1
	}
	n.Next = next
	n.mu.Unlock()
	id := (n.Id + pow(2, next-1)) % pow(2, n.M)
	reply := new(NodeAddress)
	err := n.FindSuccessor(&id, reply)
	if err != nil {
		log.Fatalf("failed to fix finger: %+v\n", err)
	}
	n.mu.Lock()
	n.FingerTable[next] = *reply
	n.mu.Unlock()
}

func (n *Node) closestPrecedingNodes(id int) []NodeAddress {
	nodes := []NodeAddress{n.createAddress()}
	for i := n.M; i >= 1; i-- {
		nodes = append(nodes, n.FingerTable[i])
	}
	nodes = append(nodes, n.Successors...)
	slices.SortFunc(nodes, func(i, j NodeAddress) int {
		return CounterClockwiseDistance(i.Id, id, n.M) - CounterClockwiseDistance(j.Id, id, n.M)
	})
	nodes = slices.CompactFunc(nodes, func(a, b NodeAddress) bool {
		return a == b
	})
	return nodes
}

func (n *Node) stabilize() {
	for _, succ := range n.Successors {
		successorList, err := callGetSuccessorList(succ)
		if err != nil {
			continue
		}
		predecessor, err := callGetPredecessor(succ)
		if err != nil {
			continue
		}

		newSuccessors := []NodeAddress{}

		predecessorArcLength := CounterClockwiseDistance(predecessor.Id, n.Id, n.M)
		succArcLength := CounterClockwiseDistance(succ.Id, n.Id, n.M)

		if predecessorNotNil(*predecessor) {
			if predecessorArcLength != 0 && predecessorArcLength < succArcLength { // is predecessor between n and succ?
				newSuccessors = append(newSuccessors, *predecessor)
			} else if succ.Id == n.Id { // needed if n has itself as successor, otherwise will never update predecessor
				newSuccessors = append(newSuccessors, *predecessor)
			}
		}

		successors := *successorList
		newSuccessors = append(newSuccessors, succ)
		newSuccessors = append(newSuccessors, successors[:len(successors)-len(newSuccessors)]...)
		n.mu.Lock()
		n.Successors = newSuccessors
		address := n.createAddress()
		n.mu.Unlock()

		callNotify(n.Successors[0], address)
		return
	}
	log.Fatalf("Shutting down. Could not stabilize with any successor: %+v\n", n.Successors)
}

func (n *Node) checkPredecessor() {
	n.mu.Lock()
	if predecessorNotNil(n.Predecessor) && !callHealthCheck(n.Predecessor) {
		n.Predecessor = NodeAddress{}
	}
	n.mu.Unlock()
}

func (n *Node) createAddress() NodeAddress {
	return NodeAddress{
		IP:   n.IP,
		Port: n.Port,
		Id:   n.Id,
	}
}

func (n *Node) shouldChangePredecessor(potentialPredecessor NodeAddress) bool {
	if predecessorIsNil(n.Predecessor) {
		return true
	}
	return IsNewCloserPredecessor(n.Id, n.Predecessor.Id, potentialPredecessor.Id, n.M)
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
