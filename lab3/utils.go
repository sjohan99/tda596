package main

import (
	"log"
	"math"
	"math/big"
)

func getIdFromHash(hash []byte, m int) int {
	n := new(big.Int).SetBytes(hash)
	res := int(n.Mod(n, big.NewInt(1<<m)).Int64())
	log.Printf("Id: %d\n", res)
	return res
}

func pow(base int, exp int) int {
	return int(math.Pow(float64(base), float64(exp)))
}
