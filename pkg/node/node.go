package node

import (
	"strings"

	"github.com/google/uuid"
)

// LineageDelim represents the delimiter for a Node's Lineage.
const LineageDelim = "/"

// Node represents a recursive struct.
type Node struct {
	ID       string `dynamodbav:"ID"`
	ParentID string `dynamodbav:"ParentID"`

	Parent   *Node   `dynamodbav:"-"`
	Children []*Node `dynamodbav:"-"`
}

// New returns a new node.
func New(parent *Node) *Node {
	id := uuid.New().String()

	n := &Node{
		Children: []*Node{},
		ID:       id,
		ParentID: id,
	}

	if parent != nil {
		parent.AddChild(n)
	}

	return n
}

// CreateChild created a child node for the given node and returns a pointer to the new child node.
func (n *Node) CreateChild() *Node {
	return New(n)
}

// AddChild adds the given Node to the receiver.
func (n *Node) AddChild(c *Node) {
	n.Children = append(n.Children, c)
	c.Parent = n
	c.ParentID = n.ID
}

// HasParent returns true if the Node is a child Node.
func (n Node) HasParent() bool {
	return n.ID != n.ParentID
}

// GetParentID parses the Node's Lineage and returns the parent Node ID.
func (n Node) GetParentID() (s string) {
	if n.HasParent() {
		s = n.ParentID
	}

	return
}

// GetRootID parses the Node's Lineage and returns the root Node ID
// func (n Node) GetRootID() (s string) {
// 	if n.HasParent() {
// 		s = strings.Split(n.Lineage, LineageDelim)[0]
// 	}

// 	return
// }

// String returns a string representation of a node and all its children.
func (n Node) String() string {
	var sb strings.Builder
	convertToString(n, &sb, "")
	return sb.String()
}

func convertToString(n Node, sb *strings.Builder, indent string) {
	sb.WriteString(indent + "ID: " + n.ID + "\n")
	// sb.WriteString(indent + "Lineage: " + n.Lineage + "\n")
	sb.WriteString(indent + "ParentID: " + n.GetParentID() + "\n")
	// sb.WriteString(indent + "RootID: " + n.RootID + "\n")

	if len(n.Children) > 0 {
		sb.WriteString(indent + "Children:\n")
		for _, c := range n.Children {
			convertToString(*c, sb, indent+"  ")
		}
	} else {
		sb.WriteString("\n")
	}
}

func joinStr(sep string, a ...string) string {
	validStrings := []string{}

	for _, s := range a {
		if strings.TrimSpace(s) != "" {
			validStrings = append(validStrings, s)
		}
	}
	return strings.Join(validStrings, sep)
}
