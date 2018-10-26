package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/erumble/dynamo-playground/pkg/node"
)

func main() {
	fmt.Println("Running node recursion test...")

	root, _ := node.New("root", nil, nil)

	for i := 0; i < 3; i++ {
		root.CreateChild(fmt.Sprintf("c-%d", i))

		for j := 0; j < 2; j++ {
			root.Children[i].CreateChild(fmt.Sprintf("%v-%d", root.Children[i].ID, j))
		}
	}

	fmt.Print(root)

	fmt.Println("Running dynamodb test...")

	// dRoot, _ := node.New("dRoot", nil, nil)

	sess, err := session.NewSession(&aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	})
	if err != nil {
		log.Fatal(err)
	}

	dynamoSvc := dynamodb.New(sess)
	client := node.NewClient(dynamoSvc, "nodes")

	fmt.Println("creating table...")
	if _, err := client.CreateTable(); err != nil {
		log.Printf("Error creating table: %v\n", err)
	}

	fmt.Println("populating table...")
	if err := client.Put(root); err != nil {
		log.Printf("Error adding node: %v\n", err)
	}

	fmt.Println("retrieving data from table...")
	if n, err := client.Get("root"); err != nil {
		log.Printf("Error fetching node: %v\n", err)
	} else {
		fmt.Print(n)
	}

	fmt.Println("deleting table...")
	if _, err := client.DeleteTable(); err != nil {
		log.Printf("Error deleting table: %v\n", err)
	}
}
