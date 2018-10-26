package node

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// DynamoDBIFace represents the method calls to DynamoDB that this package uses.
type DynamoDBIFace interface {
	BatchGetItem(*dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	DeleteTable(*dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// Client provides the concrete implementation to interact with the DynamoDBIface.
type Client struct {
	dataStore DynamoDBIFace
	tableName string
}

// NewClient creates a new Node Client to interact with DynamoDB.
func NewClient(db DynamoDBIFace, tableName string) Client {
	return Client{
		dataStore: db,
		tableName: tableName,
	}
}

// CreateTable creates the dynamodb table with the name supplied to the Client.
func (c Client) CreateTable() (*dynamodb.CreateTableOutput, error) {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			},
		},
		TableName: aws.String(c.tableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	return c.dataStore.CreateTable(input)
}

// DeleteTable deletes the dynamodb table with the name supplied to the Client.
func (c Client) DeleteTable() (*dynamodb.DeleteTableOutput, error) {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(c.tableName),
	}

	return c.dataStore.DeleteTable(input)
}

// Get fetches the Node with the given ID, along with its children, from DynamoDB.
func (c Client) Get(id string) (*Node, error) {
	n, err := New(id, nil, nil)
	if err != nil {
		fmt.Println("error creating node")
		return nil, err
	}

	av, err := dynamodbattribute.MarshalMap(n)
	if err != nil {
		fmt.Println("error marshalling node")
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		Key:       av,
		TableName: aws.String(c.tableName),
	}

	fmt.Println(input)

	res, err := c.dataStore.GetItem(input)
	if err != nil {
		fmt.Println("error retrieving node")
		return nil, err
	}

	if err = dynamodbattribute.UnmarshalMap(res.Item, n); err != nil {
		fmt.Println("error unmarshalling node")
		return nil, err
	}

	return n, nil
}

// Put stores the given Node, and all children, in DynamoDB.
func (c Client) Put(in *Node) error {
	fmt.Println("marshalling data...")
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(c.tableName),
	}

	fmt.Println("calling putitem...")
	if _, err := c.dataStore.PutItem(input); err != nil {
		return err
	}

	return nil
}

// Delete removes the given Node, and all children, from DynamoDB.
func (c Client) Delete(in *Node) error {
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		return err
	}

	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(c.tableName),
	}

	if _, err := c.dataStore.DeleteItem(input); err != nil {
		return err
	}

	return nil
}
