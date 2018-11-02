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

	for i := 0; i < 3; i++ {
		c := root.CreateChild()
		nodes = append(nodes, c)

		for j := 0; j < 2; j++ {
			gc := c.CreateChild()
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
	n, err := client.Get(root.ID)
	if err != nil {
		logger.Errorf("Error fetching node: %v", err)
	} else {
		logger.Info("Results:")
		spew.Dump(n)
	}

	logger.Info("retrieving child info for previously retrieved node...")
	if res, err := client.GetChildren(*n); err != nil {
		logger.Errorf("Error fetching node: %v", err)
	} else {
		logger.Info("Results:")
		spew.Dump(res)
	}

	logger.Info("retrieving sibling info previously retrieved node...")
	if res, err := client.GetSiblings(*n); err != nil {
		logger.Errorf("Error fetching node: %v", err)
	} else {
		logger.Info("Results:")
		spew.Dump(res)
	}

	// nodes := []*node.Node{}
	// for i := 0; i < 5; i++ {
	// 	n := node.New(nil)
	// 	nodes = append(nodes, n)
	// }

	// logger.Info("first batch write...")
	// logger.Debug(nodes)
	// if err := client.PutA(nodes); err != nil {
	// 	logger.Debugf("Error adding nodes the first time: %v\n", err)
	// }

	// fmt.Print("Press 'Enter' to continue...")
	// bufio.NewReader(os.Stdin).ReadBytes('\n')

	// for _, n := range nodes {
	// 	n.Lineage = "modified"
	// }

	// logger.Info("second batch write...")
	// logger.Debug(nodes)
	// if err := client.PutA(nodes); err != nil {
	// 	logger.Debugf("Error adding nodes the second time: %v\n", err)
	// }

	// logger.Info("")
	// m, err := node.Marshal(*root)
	// if err != nil {
	// 	fmt.Printf("Error marshalling node: %v\n", err)
	// }

	// logger.Info(m)
}
