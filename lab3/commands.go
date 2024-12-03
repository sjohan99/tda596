package main

import (
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

	fmt.Println("Predecessor:")
	fmt.Printf("\tId: %d\n", nCopy.Predecessor.Id)

	fmt.Println("Finger Table:")
	for i := 1; i <= nCopy.M; i++ {
		fmt.Printf("\tFinger %d (=%d): %+v\n", i, (nCopy.Id+pow(2, i-1))%pow(2, nCopy.M), nCopy.FingerTable[i])
	}

	fmt.Println("Files:")
	for id, file := range nCopy.Files {
		fmt.Printf("\t%s (id=%d)\n", file.Name, id)
	}
}

func (n *Node) LookUpCmd(filename string) {
	node, fileId, err := n.LookUp(filename)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	reply := new(GetFileReply)
	ok := callGetFile(*node, &fileId, reply)
	if !ok {
		fmt.Printf("Could not look up file. Failed to reach node %d", node.Id)
		return
	}
	if reply.ErrorMessage != "" {
		fmt.Printf("Error: %s\n", reply.ErrorMessage)
		return
	}
	fmt.Printf("File '%s' is stored at node:\n \tid=%d\n\tip=%s\n\tport=%s\n", filename, node.Id, node.IP, node.Port)
	fmt.Printf("File contents: \n%s\n", reply.Data)
}

func (n *Node) StoreFileCmd(filename string) {
	targetNode, fileId, err := n.LookUp(filename)
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
		Id:   fileId,
		Name: filename,
		Data: string(data),
	}
	ok := callStoreFile(*targetNode, &args)
	if !ok {
		fmt.Println("Error: Failed to store file.")
		return
	}
	fmt.Printf("File '%s' stored at node id=%d\n", filename, targetNode.Id)
}
