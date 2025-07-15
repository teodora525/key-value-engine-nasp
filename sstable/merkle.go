package sstable

import (
	"crypto/sha256"
)

type MerkleNode struct {
	Left  *MerkleNode
	Right *MerkleNode
	Hash  []byte
}

type MerkleTree struct {
	Root *MerkleNode
}

// Kreiraj čvor iz vrednosti (leaf)
func NewMerkleNode(left, right *MerkleNode, data []byte) *MerkleNode {
	node := &MerkleNode{}

	if left == nil && right == nil {
		hash := sha256.Sum256(data)
		node.Hash = hash[:]
	} else {
		prevHashes := append(left.Hash, right.Hash...)
		hash := sha256.Sum256(prevHashes)
		node.Hash = hash[:]
	}

	node.Left = left
	node.Right = right

	return node
}

// Kreiraj celo stablo iz liste vrednosti
func NewMerkleTree(data [][]byte) *MerkleTree {
	var nodes []MerkleNode

	// Leaf nodes
	for _, datum := range data {
		node := NewMerkleNode(nil, nil, datum)
		nodes = append(nodes, *node)
	}

	// Ako je neparan broj, dupliraj zadnji
	if len(nodes)%2 != 0 {
		nodes = append(nodes, nodes[len(nodes)-1])
	}

	// Gradnja stabla odozdo
	for len(nodes) > 1 {
		var level []MerkleNode

		for i := 0; i < len(nodes); i += 2 {
			left := &nodes[i]
			right := &nodes[i+1]
			parent := NewMerkleNode(left, right, nil)
			level = append(level, *parent)
		}

		nodes = level

		if len(nodes)%2 != 0 && len(nodes) > 1 {
			nodes = append(nodes, nodes[len(nodes)-1])
		}
	}

	tree := MerkleTree{&nodes[0]}
	return &tree
}
