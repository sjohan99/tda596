package main_test

import (
	c "chord"
	a "chord/argparser"
	"context"
	"log"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"
)

var backgroundCtx = context.Background()

func makeConfig(id int, port int) a.Config {
	calcFunc := func(hash []byte, m int) int {
		return id
	}

	return a.Config{
		Address:                  "localhost",
		Port:                     strconv.Itoa(port),
		StabilizeInterval:        433,
		FixFingersInterval:       223,
		CheckPredecessorInterval: 449,
		Successors:               4,
		CalculateIdFunc:          calcFunc,
		M:                        6,
	}
}

func makeJoinConfig(id int, port int, c a.Config) a.Config {
	config := makeConfig(id, port)
	config.JoinAddress = c.Address
	config.JoinPort = c.Port
	return config
}

func getSuccessorIds(node *c.Node) []int {
	ids := make([]int, len(node.Successors))
	for i, succ := range node.Successors {
		ids[i] = succ.Id
	}
	return ids
}

func getFingerTableIds(node *c.Node) []int {
	keys := make([]int, 0)
	for k := range node.FingerTable {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	ids := make([]int, 0)
	for _, k := range keys {
		ids = append(ids, node.FingerTable[k].Id)
	}
	return ids
}

func expectSuccessorIds(expected []int, n *c.Node, t *testing.T) {
	successors := getSuccessorIds(n)
	if slices.Compare(successors, expected) != 0 {
		t.Errorf("Expected node %d to have successors %v, got %v", n.Id, expected, successors)
	}
}

func expectFingerIds(expected []int, n *c.Node, t *testing.T) {
	fingers := getFingerTableIds(n)
	if slices.Compare(fingers, expected) != 0 {
		t.Errorf("Expected node %d to have fingers %v, got %v", n.Id, expected, fingers)
	}
}

func expectPredecessor(expected int, n *c.Node, t *testing.T) {
	if n.Predecessor.Id != expected {
		t.Errorf("Expected node %d to have predecessor with ID %d, got id=%d", n.Id, expected, n.Predecessor.Id)
	}
}

func TestMain(m *testing.M) {
	//log.SetOutput(io.Discard)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	m.Run()
}

func TestChordOneNode(t *testing.T) {
	config := makeConfig(5, 5000)
	config.M = 4
	node := c.CreateNode(config)
	node.Start(config, &backgroundCtx)

	time.Sleep(3 * time.Second)

	nodeState := node.GetState()

	expectSuccessorIds([]int{5, 5, 5, 5}, &nodeState, t)
	expectFingerIds([]int{5, 5, 5, 5}, &nodeState, t)
	expectPredecessor(5, &nodeState, t)
}

func TestChordTwoNodes(t *testing.T) {
	config1 := makeConfig(41, 5001)
	config2 := makeJoinConfig(5, 5002, config1)

	node1 := c.CreateNode(config1)
	node1.Start(config1, &backgroundCtx)
	node2 := c.JoinNode(config2)
	node2.Start(config2, &backgroundCtx)

	time.Sleep(5 * time.Second)

	node1State := node1.GetState()
	node2State := node2.GetState()

	expectSuccessorIds([]int{5, 41, 5, 41}, &node1State, t)
	expectFingerIds([]int{5, 5, 5, 5, 5, 41}, &node1State, t)
	expectSuccessorIds([]int{41, 5, 41, 5}, &node2State, t)
	expectFingerIds([]int{41, 41, 41, 41, 41, 41}, &node2State, t)

	expectPredecessor(5, &node1State, t)
	expectPredecessor(41, &node2State, t)
}

func TestChordFourNodes(t *testing.T) {
	config1 := makeConfig(41, 5003)
	config2 := makeJoinConfig(21, 5004, config1)
	config3 := makeJoinConfig(40, 5005, config2)
	config4 := makeJoinConfig(56, 5006, config3)

	node1 := c.CreateNode(config1)
	node1.Start(config1, &backgroundCtx)
	node2 := c.JoinNode(config2)
	node2.Start(config2, &backgroundCtx)
	node3 := c.JoinNode(config3)
	node3.Start(config3, &backgroundCtx)
	node4 := c.JoinNode(config4)
	node4.Start(config4, &backgroundCtx)

	time.Sleep(5 * time.Second)

	node1State := node1.GetState()
	node2State := node2.GetState()
	node3State := node3.GetState()
	node4State := node4.GetState()

	expectSuccessorIds([]int{56, 21, 40, 41}, &node1State, t)
	expectFingerIds([]int{56, 56, 56, 56, 21, 21}, &node1State, t)

	expectSuccessorIds([]int{40, 41, 56, 21}, &node2State, t)
	expectFingerIds([]int{40, 40, 40, 40, 40, 56}, &node2State, t)

	expectSuccessorIds([]int{41, 56, 21, 40}, &node3State, t)
	expectFingerIds([]int{41, 56, 56, 56, 56, 21}, &node3State, t)

	expectSuccessorIds([]int{21, 40, 41, 56}, &node4State, t)
	expectFingerIds([]int{21, 21, 21, 21, 21, 40}, &node4State, t)

	expectPredecessor(40, &node1State, t)
	expectPredecessor(56, &node2State, t)
	expectPredecessor(21, &node3State, t)
	expectPredecessor(41, &node4State, t)
}

func TestChordFourNodesWithOneNodeFailing(t *testing.T) {
	config1 := makeConfig(41, 5007)
	config2 := makeJoinConfig(21, 5008, config1)
	config3 := makeJoinConfig(40, 5009, config2)
	config4 := makeJoinConfig(56, 5010, config3)

	ctx, cancel := context.WithCancel(context.Background())

	node1 := c.CreateNode(config1)
	node1.Start(config1, &ctx)
	node2 := c.JoinNode(config2)
	node2.Start(config2, &backgroundCtx)
	node3 := c.JoinNode(config3)
	node3.Start(config3, &backgroundCtx)
	node4 := c.JoinNode(config4)
	node4.Start(config4, &backgroundCtx)

	time.Sleep(5 * time.Second)

	cancel() // stop node1

	time.Sleep(5 * time.Second)

	node2State := node2.GetState()
	node3State := node3.GetState()
	node4State := node4.GetState()

	expectSuccessorIds([]int{40, 56, 21, 40}, &node2State, t)
	expectFingerIds([]int{40, 40, 40, 40, 40, 56}, &node2State, t)

	expectSuccessorIds([]int{56, 21, 40, 56}, &node3State, t)
	expectFingerIds([]int{56, 56, 56, 56, 56, 21}, &node3State, t)

	expectSuccessorIds([]int{21, 40, 56, 21}, &node4State, t)
	expectFingerIds([]int{21, 21, 21, 21, 21, 40}, &node4State, t)

	expectPredecessor(56, &node2State, t)
	expectPredecessor(21, &node3State, t)
	expectPredecessor(40, &node4State, t)
}
