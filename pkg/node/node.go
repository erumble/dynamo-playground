package node

import (
	"github.com/google/uuid"
)

// Node represents a recursive struct.
type Node struct {
	ID       string   `dynamodbav:"ID"`
	ParentID string   `dynamodbav:",omitempty"`
	ChildIDs []string `dynamodbav:",omitempty,omitemptyelem"`
	Metadata string   `dynamodbav:",omitempty"`
}

// New creates a new node.
// If the parent param is non-nil, it will create a new child node for the parent.
func New(parent *Node) *Node {
	id := uuid.New().String()

	n := &Node{
		ID:       id,
		ChildIDs: []string{},
	}

	if parent != nil {
		parent.RegisterChild(n)
	}

	return n
}

// CreateChild created a child node for the given parent node,
// and returns a pointer to the new child node.
func (n *Node) CreateChild() *Node {
	return New(n)
}

// RegisterChild adds the given Node to the receiver.
func (n *Node) RegisterChild(c *Node) {
	n.ChildIDs = append(n.ChildIDs, c.ID)
	c.ParentID = n.ID
}

// HasParent returns true if the Node is a child Node.
func (n Node) HasParent() bool {
	return n.ParentID != ""
}

// HasChildren returns true if the node has children.
func (n Node) HasChildren() bool {
	return len(n.ChildIDs) > 0
}
