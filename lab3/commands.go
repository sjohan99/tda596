package main

import "fmt"

func (n *Node) PrintState() {
	nCopy := n.copyNodeState()
	fmt.Println("Node:")
	fmt.Printf("\tId: %d\n", nCopy.Id)
	fmt.Printf("\tAddress: %s:%s\n", nCopy.IP, nCopy.Port)

	fmt.Println("Successors:")
	for i, succ := range nCopy.Successors {
		fmt.Printf("\tSuccessor %d: %+v\n", i, succ)
	}

	fmt.Println("Predecessors:")
	fmt.Printf("\tId: %d\n", nCopy.Predecessor.Id)

	fmt.Println("Finger Table:")
	for i := 1; i <= nCopy.M; i++ {
		fmt.Printf("\tFinger %d (=%d): %+v\n", i, (nCopy.Id+pow(2, i-1))%pow(2, nCopy.M), nCopy.FingerTable[i])
	}
}
