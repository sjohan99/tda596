package main

import "fmt"

func (n *Node) PrintState() {
	fmt.Println("Node:")
	fmt.Printf("\tId: %d\n", n.Id)
	fmt.Printf("\tAddress: %s:%s\n", n.IP, n.Port)

	fmt.Println("Successors:")
	for i, succ := range n.Successors {
		fmt.Printf("\tSuccessor %d: %+v\n", i, succ)
	}

	fmt.Println("Predecessors:")
	fmt.Printf("\tId: %d\n", n.Predecessor.Id)

	fmt.Println("Finger Table:")
	for i := 1; i <= n.M; i++ {
		fmt.Printf("\tFinger %d (=%d): %+v\n", i, n.Id+2<<(i-1)%(2<<n.M), n.FingerTable[i])
	}
}
