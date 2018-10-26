package node

import (
	"errors"
	"strings"
)

// Node represents a recursive struct.
type Node struct {
	ID       string  `dynamodbav:"ID"`
	Children []*Node `dynamodbav:"Children,omitempty,omitemptyelem"`
	Parent   *Node   `dynamodbav:"-"`
}

// New returns a new node.
func New(id string, parent *Node, children []*Node) (*Node, error) {
	if id == "" {
		return nil, errors.New("id can't be an empty string")
	}

	c := []*Node{}
	if children != nil && len(children) > 0 {
		c = children
	}

	return &Node{
		ID:       id,
		Children: c,
		Parent:   parent,
	}, nil
}

// CreateChild created a child node for the given node and returns a pointer to the new child node.
func (n *Node) CreateChild(id string) (*Node, error) {
	c, err := New(id, n, nil)
	if err != nil {
		return nil, err
	}

	n.Children = append(n.Children, c)
	return c, nil
}

// String returns a string representation of a node and all its children.
func (n *Node) String() string {
	var sb strings.Builder
	convertToString(*n, &sb, "")
	return sb.String()
}

func convertToString(n Node, sb *strings.Builder, indent string) {
	var p string
	if n.Parent == nil {
		p = "none"
	} else {
		p = n.Parent.ID
	}

	sb.WriteString(indent + "ID: " + n.ID + ", Parent: " + p + "\n")

	if len(n.Children) > 0 {
		sb.WriteString(indent + "Children:\n")
		for _, c := range n.Children {
			convertToString(*c, sb, indent+"  ")
		}
	}
}
