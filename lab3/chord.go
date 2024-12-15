package main

import (
	"chord/argparser"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"sync"
)

type NodeAddress struct {
	IP   string
	Port string
	Id   int
}

type File struct {
	Name string
	Data string
}

type Node struct {
	sync.Mutex
	Next            int                   // next finger to fix
	FingerTable     map[int]NodeAddress   // finger table
	Id              int                   // the node's identifier in the ring
	Successors      []NodeAddress         // the node's successors
	Predecessor     NodeAddress           // the node's predecessor
	IP              string                // the node's IP address
	Port            string                // the node's port
	M               int                   // the ring size = 2^M
	CalculateIdFunc func([]byte, int) int // function to calculate the ID from its hash
	Files           map[int]File          // the files stored at the node
}

func CreateNode(c argparser.Config) *Node {
	node := Node{
		Next:            0,
		FingerTable:     make(map[int]NodeAddress),
		Id:              c.CalculateIdFunc(c.Id, c.M),
		Successors:      make([]NodeAddress, c.Successors),
		Predecessor:     NodeAddress{},
		IP:              c.Address,
		Port:            c.Port,
		M:               c.M,
		CalculateIdFunc: c.CalculateIdFunc,
		Files:           make(map[int]File),
	}

	addr := node.createAddress()
	for i := 0; i < c.Successors; i++ {
		node.Successors[i] = addr
	}
	for i := 1; i <= node.M; i++ {
		node.FingerTable[i] = addr
	}

	return &node
}

func JoinNode(c argparser.Config) *Node {
	node := CreateNode(c)

	joinAddress := NodeAddress{IP: c.JoinAddress, Port: c.JoinPort, Id: node.CalculateIdFunc(c.JoinId, c.M)}
	reply, err := callFindSuccessor(joinAddress, &node.Id)
	if err != nil {
		log.Fatal("failed to join ring:", err)
	}

	for i := range node.Successors {
		node.Successors[i] = *reply
	}

	return node
}

func (n *Node) Start(c argparser.Config, ctx *context.Context) {
	if ctx == nil {
		bgCtx := context.Background()
		ctx = &bgCtx
	}
	n.StartServer(n.IP, n.Port, ctx)
	n.startBackgroundTask(ctx, c.StabilizeInterval, n.stabilize)
	n.startBackgroundTask(ctx, c.FixFingersInterval, n.fixFingers)
	n.startBackgroundTask(ctx, c.CheckPredecessorInterval, n.checkPredecessor)
}

func (n *Node) HealthCheck(_ *struct{}, _ *struct{}) error {
	return nil
}

func (n *Node) GetSuccsAndPred(_ *struct{}, reply *SuccsAndPredReply) error {
	n.Lock()
	defer n.Unlock()
	reply.Successors = n.Successors
	reply.Predecessor = n.Predecessor
	return nil
}

func (n *Node) Notify(potentialPredecessor *NodeAddress, _ *struct{}) error {
	n.Lock()
	changed := false
	if n.shouldChangePredecessor(*potentialPredecessor) {
		n.Predecessor = *potentialPredecessor
		changed = true
	}
	n.Unlock()
	if changed {
		n.migrateFilesTo(*potentialPredecessor)
	}
	return nil
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

func (n *Node) StoreFile(args *StoreFileArgs, reply *struct{}) error {
	n.Lock()
	n.Files[args.Id] = File{Name: args.Name, Data: args.Data}
	n.Unlock()
	return nil
}

func (n *Node) GetFile(fileId *int, reply *GetFileReply) error {
	n.Lock()
	file, ok := n.Files[*fileId]
	n.Unlock()
	if !ok {
		reply.ErrorMessage = "File does not exist in Chord ring."
		return nil
	}
	reply.Data = file.Data
	return nil
}

func (n *Node) LookUp(filename string) (*NodeAddress, int, error) {
	hasher := sha1.New()
	hasher.Write([]byte(filename))
	hash := hasher.Sum(nil)
	id := n.CalculateIdFunc(hash, n.M)
	reply := new(NodeAddress)
	err := n.FindSuccessor(&id, reply)
	if err != nil {
		message := fmt.Sprintf("Could not find any node for file '%s' with id=%d\n", filename, id)
		return nil, -1, errors.New(message)
	}
	return reply, id, nil
}

func (n *Node) fixFingers() {
	n.Lock()
	next := n.Next + 1
	if next > n.M {
		next = 1
	}
	n.Next = next
	n.Unlock()
	id := (n.Id + pow(2, next-1)) % pow(2, n.M)
	reply := new(NodeAddress)
	err := n.FindSuccessor(&id, reply)
	if err != nil {
		log.Fatalf("failed to fix finger: %+v\n", err)
	}
	n.Lock()
	n.FingerTable[next] = *reply
	n.Unlock()
}

func (n *Node) closestPrecedingNodes(id int) []NodeAddress {
	nodes := []NodeAddress{n.createAddress()}
	for i := n.M; i >= 1; i-- {
		nodes = append(nodes, n.FingerTable[i])
	}
	nodes = append(nodes, n.Successors...)
	slices.SortFunc(nodes, func(i, j NodeAddress) int {
		return CounterClockwiseDistance(j.Id, id, n.M) - CounterClockwiseDistance(i.Id, id, n.M)
	})
	nodes = slices.CompactFunc(nodes, func(a, b NodeAddress) bool {
		return a == b
	})
	return nodes
}

func (n *Node) stabilize() {
	for _, succ := range n.Successors {
		successorList, predecessor, err := callGetSuccsAndPred(succ)
		if err != nil {
			log.Printf("failed to get successors and predecessor: %+v\n", err)
			continue
		}

		newSuccessors := []NodeAddress{}
		predecessorDist := CounterClockwiseDistance(predecessor.Id, n.Id, n.M)
		successorDist := CounterClockwiseDistance(succ.Id, n.Id, n.M)

		if predecessorNotNil(*predecessor) {
			if predecessorDist != 0 && predecessorDist < successorDist { // is predecessor between n and succ?
				newSuccessors = append(newSuccessors, *predecessor)
			} else if succ.Id == n.Id { // needed if n has itself as successor, otherwise will never update predecessor
				newSuccessors = append(newSuccessors, *predecessor)
			}
		}

		successors := *successorList
		newSuccessors = append(newSuccessors, succ)
		newSuccessors = append(newSuccessors, successors[:len(successors)-len(newSuccessors)]...)

		n.Lock()
		n.Successors = newSuccessors
		address := n.createAddress()
		n.Unlock()

		callNotify(n.Successors[0], address)
		return
	}
	log.Fatalf("Shutting down. Could not stabilize with any successor: %+v\n", n.Successors)
}

func (n *Node) checkPredecessor() {
	n.Lock()
	if predecessorNotNil(n.Predecessor) && !callHealthCheck(n.Predecessor) {
		n.Predecessor = NodeAddress{}
	}
	n.Unlock()
}

// Migrate files which (if any) should belong to the new predecessor instead.
func (n *Node) migrateFilesTo(target NodeAddress) {
	filesToMigrate := make(map[int]File)
	n.Lock()
	for fileId, file := range n.Files {
		myDistance := CounterClockwiseDistance(n.Id, fileId, n.M)
		targetDistance := CounterClockwiseDistance(target.Id, fileId, n.M)
		if myDistance > targetDistance { // true -> target is new successor of file
			filesToMigrate[fileId] = file
			delete(n.Files, fileId)
		}
	}
	n.Unlock()
	for id, file := range filesToMigrate {
		args := StoreFileArgs{Id: id, Name: file.Name, Data: file.Data}
		callStoreFile(target, &args)
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

func callGetSuccsAndPred(node NodeAddress) (*[]NodeAddress, *NodeAddress, error) {
	reply := new(SuccsAndPredReply)
	ok := Call("Node.GetSuccsAndPred", node.IP, node.Port, new(struct{}), reply)
	if !ok {
		return nil, nil, errors.New("failed to call GetSuccsAndPred for node " + strconv.Itoa(node.Id))
	}
	return &reply.Successors, &reply.Predecessor, nil
}

func callHealthCheck(node NodeAddress) bool {
	return Call("Node.HealthCheck", node.IP, node.Port, new(struct{}), new(struct{}))
}

func callNotify(node NodeAddress, n NodeAddress) {
	Call("Node.Notify", node.IP, node.Port, n, new(struct{}))
}

func callStoreFile(node NodeAddress, args *StoreFileArgs) bool {
	return Call("Node.StoreFile", node.IP, node.Port, args, new(struct{}))
}

func callGetFile(node NodeAddress, fileId *int, reply *GetFileReply) bool {
	return Call("Node.GetFile", node.IP, node.Port, fileId, reply)
}
