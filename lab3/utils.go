package main

import (
	"context"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"time"
)

func pow(base int, exp int) int {
	return int(math.Pow(float64(base), float64(exp)))
}

func predecessorNotNil(predecessor NodeAddress) bool {
	return predecessor != NodeAddress{}
}

func predecessorIsNil(predecessor NodeAddress) bool {
	return predecessor == NodeAddress{}
}

func makeFilePath(filename string, id int) string {
	prefix := strconv.Itoa(id)
	filepath := path.Join("data", prefix, filename)
	if err := os.MkdirAll(path.Dir(filepath), os.ModePerm); err != nil {
		log.Fatalf("failed to create directory: %v", err)
	}
	return filepath
}

func fillReply(reply *NodeAddress, node NodeAddress) {
	reply.IP = node.IP
	reply.Port = node.Port
	reply.Id = node.Id
}

func (n *Node) copyNodeState() Node {
	n.Lock()
	defer n.Unlock()
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
		Files:       n.Files,
	}
}

func (n *Node) createAddress() NodeAddress {
	return NodeAddress{
		IP:   n.IP,
		Port: n.Port,
		Id:   n.Id,
	}
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

// Check if the new node is closer (counter clockwise) to the current node than the old node
func IsNewCloserPredecessor(node, old, new, m int) bool {
	if node == old { // always replace if node's predecessor is itself
		return true
	}

	if CounterClockwiseDistance(node, new, m) < CounterClockwiseDistance(node, old, m) {
		return true
	}
	return false
}

func CounterClockwiseDistance(from, to, m int) int {
	if from == to {
		return 0
	}

	ringSize := pow(2, m)
	if from > to {
		return from - to
	}
	return from + ringSize - to
}

func (n *Node) GetState() Node {
	return n.copyNodeState()
}
