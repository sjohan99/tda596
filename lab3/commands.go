package main

import (
	"crypto/sha1"
	"fmt"
	"os"
)

func (n *Node) PrintStateCmd() {
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

func (n *Node) LookUp(filename string) (*NodeAddress, error) {
	// "hello.txt" -> 90a9sd09asd80912830918 -> 34
	hasher := sha1.New()
	hasher.Write([]byte(filename))
	hash := hasher.Sum(nil)
	id := n.CalculateIdFunc(hash, n.M)
	fmt.Printf("file has id=%d\n", id)
	reply := new(NodeAddress)
	err := n.FindSuccessor(&id, reply)
	if err != nil {
		fmt.Printf("Failed to find the successor for file '%s' with id=%d\n", filename, id)
		return nil, err
	}
	return reply, nil
}

func (n *Node) LookUpCmd(filename string) {
	node, err := n.LookUp(filename)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	reply := new([]byte)
	ok := callGetFile(*node, &filename, reply)
	if !ok {
		fmt.Println("Error: Failed to get file.")
		return
	}
	fmt.Printf("File '%s' is stored at node id=%d, ip=%s, port=%s\n", filename, node.Id, node.IP, node.Port)
	fmt.Printf("File contents: \n%s\n", string(*reply))
}

func (n *Node) StoreFileCmd(filename string) {
	targetNode, err := n.LookUp(filename)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	args := StoreFileArgs{
		Filename: filename,
		Data:     data,
	}
	ok := callStoreFile(*targetNode, &args)
	if !ok {
		fmt.Println("Error: Failed to store file.")
	}
}
