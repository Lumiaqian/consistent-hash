package consistenthash

import "testing"

// consistent_hash_test.go

func TestGetNodeAfterRemovingNode(t *testing.T) {
	const numNodes = 3
	nodes := []string{"node1", "node2", "node3"}

	consistentHash := NewConsistentHash(numNodes)
	for _, node := range nodes {
		consistentHash.AddNode(node)
	}

	key := "key1"
	expectedNode := "node2"

	node := consistentHash.GetNode(key)
	if node != expectedNode {
		t.Errorf("Expected node %s, got %s", expectedNode, node)
	}

	consistentHash.RemoveNode("node1")

	node = consistentHash.GetNode(key)
	if node != expectedNode {
		t.Errorf("Expected node %s, got %s", expectedNode, node)
	}
}
