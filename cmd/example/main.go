package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/davecgh/go-spew/spew"
	"github.com/erumble/dynamo-playground/pkg/logger"
	"github.com/erumble/dynamo-playground/pkg/node"
)

func main() {
	logLevel := "info"
	logger := logger.NewLeveledLogger(&logLevel)

	logger.Info("Generating node tree...")
	nodes := []*node.Node{node.New(nil)}

	root := nodes[0]
	root.Metadata = "some old data"

	for i := 0; i < 3; i++ {
		c := root.CreateChild()
		c.Metadata = "some old data"
		nodes = append(nodes, c)

		for j := 0; j < 2; j++ {
			gc := c.CreateChild()
			gc.Metadata = "some old data"
			nodes = append(nodes, gc)
		}
	}

	logger.Debugf("generated tree:\n%+v", nodes)

	logger.Info("Running dynamodb test...")

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "personal"),
	}))

	dynamoSvc := dynamodb.New(sess)
	client := node.NewClient(logger, dynamoSvc, "nodes", "ParentID-index")

	logger.Info("populating table...")
	if err := client.BatchPut(nodes); err != nil {
		logger.Debugf("Error adding node: %v", err)
	}

	logger.Info("retrieving data from table by ID...")
	rootRes, err := client.Get(root.ID)
	if err != nil {
		logger.Errorf("Error fetching node: %v", err)
	} else {
		logger.Info("Results:")
		spew.Dump(rootRes)
	}

	logger.Info("retrieving child info for previously retrieved node...")
	childRes, err := client.GetChildren(*rootRes)
	if err != nil {
		logger.Errorf("Error fetching node: %v", err)
	} else {
		logger.Info("Results:")
		spew.Dump(childRes)
		for _, c := range childRes {
			c.Metadata = "some new data"
		}
	}

	logger.Info("retrieving sibling info previously retrieved node...")
	siblingRes, err := client.GetSiblings(*rootRes)
	if err != nil {
		logger.Errorf("Error fetching node: %v", err)
	} else {
		logger.Info("Results:")
		spew.Dump(siblingRes)
		for _, s := range siblingRes {
			s.Metadata = "some new data"
		}
	}

	toWrite := []*node.Node{}
	toWrite = append(toWrite, rootRes)
	toWrite = append(toWrite, childRes...)
	toWrite = append(toWrite, siblingRes...)

	logger.Info("updating table...")
	spew.Dump(toWrite)

	if err := client.BatchPut(toWrite); err != nil {
		logger.Errorf("Error updating table: %v", err)
	}
}
