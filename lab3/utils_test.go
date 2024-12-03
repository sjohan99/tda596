package main_test

import (
	c "chord"
	"testing"
)

const m = 6

var counterClockwiseDistanceTests = []struct {
	from, to, expected int
}{
	{41, 21, 20},
	{41, 5, 36},
	{41, 41, 0},
	{41, 62, 43},
	{5, 41, 28},
}

func TestCounterClockWiseDistance(t *testing.T) {
	for _, test := range counterClockwiseDistanceTests {
		if output := c.CounterClockwiseDistance(test.from, test.to, m); output != test.expected {
			t.Errorf("Test failed: %v inputted, %v expected, %v received", test, test.expected, output)
		}
	}
}

var isNewCloserPredecessorTests = []struct {
	node, old, new int
	expected       bool
}{
	{41, 21, 20, false},
	{41, 5, 21, true},
	{41, 62, 5, true},
	{5, 62, 41, false},
	{5, 21, 4, true},
	{41, 41, 42, true},
}

func TestIsNewCloserPredecessor(t *testing.T) {
	for _, test := range isNewCloserPredecessorTests {
		if output := c.IsNewCloserPredecessor(test.node, test.old, test.new, m); output != test.expected {
			t.Errorf("Test failed: %v inputted, %v expected, %v received", test, test.expected, output)
		}
	}
}
