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

func isInRange(value, start, end int) bool {
	if start <= end {
		return value >= start && value <= end
	}
	return value >= start || value <= end
}
