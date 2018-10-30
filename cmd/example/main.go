package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/erumble/dynamo-playground/pkg/node"
)

func main() {
	// fmt.Println("Running node recursion test...")

	// root := node.New(nil)

	// for i := 0; i < 3; i++ {
	// 	root.CreateChild()

	// 	for j := 0; j < 2; j++ {
	// 		root.Children[i].CreateChild()
	// 	}
	// }

	// fmt.Print(root)

	// if avs, err := node.Marshal(*root); err != nil {
	// 	log.Printf("Error marshalling node: %v\n", err)
	// } else {
	// 	log.Println("Marshalled node struct")
	// 	log.Println(avs)
	// }

	fmt.Println("Running dynamodb test...")

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", "personal"),
	}))

	dynamoSvc := dynamodb.New(sess)
	client := node.NewClient(dynamoSvc, "nodes")

	// nodes := []*node.Node{}
	// for i := 0; i < 5; i++ {
	// 	n := node.New(nil)
	// 	nodes = append(nodes, n)
	// }

	// fmt.Println("first batch write...")
	// fmt.Println(nodes)
	// if err := client.PutA(nodes); err != nil {
	// 	log.Printf("Error adding nodes the first time: %v\n", err)
	// }

	// fmt.Print("Press 'Enter' to continue...")
	// bufio.NewReader(os.Stdin).ReadBytes('\n')

	// for _, n := range nodes {
	// 	n.Lineage = "modified"
	// }

	// fmt.Println("second batch write...")
	// fmt.Println(nodes)
	// if err := client.PutA(nodes); err != nil {
	// 	log.Printf("Error adding nodes the second time: %v\n", err)
	// }

	// fmt.Println("populating table...")
	// if err := client.BatchPut(root); err != nil {
	// 	log.Printf("Error adding node and children: %v\n", err)
	// }

	// if err := client.Put(root); err != nil {
	// 	log.Printf("Error adding node: %v\n", err)
	// }

	fmt.Println("retrieving data from table by ID...")
	if n, err := client.Query("1574ec65-ac54-4c6d-b146-804cd4487745", node.ID); err != nil {
		log.Printf("Error fetching node: %v\n", err)
	} else {
		log.Printf("Results: \n%v\n", n)
	}

	fmt.Println("retrieving data from table by ParentID...")
	if n, err := client.Query("811199eb-bc4b-4d71-a1f6-7b17304541f7", node.ParentID); err != nil {
		log.Printf("Error fetching node: %v\n", err)
	} else {
		log.Printf("Results: \n%v\n", n)
	}

	// m, err := node.Marshal(*root)
	// if err != nil {
	// 	fmt.Printf("Error marshalling node: %v\n", err)
	// }

	// fmt.Println(m)
}
