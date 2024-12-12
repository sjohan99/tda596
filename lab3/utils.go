package main

import (
	"log"
	"math"
	"os"
	"path"
	"strconv"
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
