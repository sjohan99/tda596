package main

import (
	"math"
	"math/big"
)

func getIdFromHash(hash []byte, m int) int {
	n := new(big.Int).SetBytes(hash)
	res := int(n.Mod(n, big.NewInt(1<<m)).Int64())
	return res
}

func pow(base int, exp int) int {
	return int(math.Pow(float64(base), float64(exp)))
}

func predecessorNotNil(predecessor NodeAddress) bool {
	return predecessor != NodeAddress{}
}

func predecessorIsNil(predecessor NodeAddress) bool {
	return predecessor == NodeAddress{}
}

func isInRange(value, start, end int) bool {
	if start <= end {
		return value >= start && value <= end
	}
	return value >= start || value <= end
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

// Returns the closest successor (clockwise) to the given node between a and b
func closestSuccessor(node, a, b, m int) int {
	ringSize := pow(2, m)
	if a < node {
		a += ringSize
	}
	if b < node {
		b += ringSize
	}
	if a < b {
		return a
	}
	return b
}
