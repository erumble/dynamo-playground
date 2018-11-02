package node

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/erumble/dynamo-playground/pkg/logger"
	"github.com/pkg/errors"
)

// DynamoDBIFace represents the method calls to DynamoDB that this package uses.
type DynamoDBIFace interface {
	BatchGetItem(*dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	Query(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
}

// Client provides the concrete implementation to interact with the DynamoDBIface.
type Client struct {
	dataStore DynamoDBIFace
	log       logger.LeveledLogger

	gsiName   string
	tableName string
}

// NewClient creates a new Node Client to interact with DynamoDB.
//
// Parameters:
//   - logger: A concrete implementation for the logger.LeveledLogger interface.
//             It is designed for the zap.SugaredLogger https://godoc.org/go.uber.org/zap
//   - db: A concrete implementation for the DynamoDBIFace interface.
//         This is normally an aws dynamodb.DynamoDB.
//   - tableName: The name of the table to query in DynamoDB. The table should
//                have a partition key set as ID, with no range key.
//   - gsiName: The name of the GSI in for the given table in DynamoDB.
//              The GSI should have the partition key set as ParentID, and the
//              range key set as the ID, both are strings.
func NewClient(logger logger.LeveledLogger, db DynamoDBIFace, tableName string, gsiName string) Client {
	return Client{
		dataStore: db,
		log:       logger.Indent("nodeClient"),
		gsiName:   gsiName,
		tableName: tableName,
	}
}

// Get fetches the Node with the given ID from DynamoDB.
// It does not fetch or associate child nodes.
func (c Client) Get(id string) (*Node, error) {
	log := c.log.Indent("Get")
	log.Debug("called...")
	defer log.Debug("exited")
	// A node has to be created to unmarshal the results from dynamo, might as
	// well use it to marshal the attribute value map used by the GetItem method.
	n := &Node{ID: id}

	log.Debug("generating GetItemInput...")
	// This can't error
	av, _ := dynamodbattribute.MarshalMap(n)

	input := &dynamodb.GetItemInput{
		Key:       av,
		TableName: aws.String(c.tableName),
	}

	log.Debugf("GetItemInput:\n%v", input)

	log.Debug("calling GetItem...")
	res, err := c.dataStore.GetItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "Client.Get: Error retrieving node")
	}

	log.Debug("unmarshalling results...")
	if err = dynamodbattribute.UnmarshalMap(res.Item, n); err != nil {
		return nil, errors.Wrap(err, "Client.Get: Error unmarshalling results into type Node")
	}

	return n, nil
}

// BatchGet fetches the nodes with the given IDs from DynamoDB.
// It does not fetch or associate child nodes.
func (c Client) BatchGet(ids []string) ([]*Node, error) {
	log := c.log.Indent("BatchGet")
	log.Debug("called...")
	defer log.Debug("exited")

	log.Debug("generating BatchGetItemInput...")
	avs := []map[string]*dynamodb.AttributeValue{}
	for _, id := range ids {
		avs = append(avs, map[string]*dynamodb.AttributeValue{
			"ID": &dynamodb.AttributeValue{
				S: aws.String(id),
			},
		})
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			c.tableName: {
				Keys: avs,
			},
		},
	}

	log.Debugf("BatchGetItemInput:\n%v", input)

	log.Debug("calling BatchGetItem...")
	res, err := c.dataStore.BatchGetItem(input)
	if err != nil {
		return nil, errors.Wrap(err, "Client.BatchGet: error retrieving data from dynamodb")
	}

	log.Debug("unmarshalling results...")

	return unmarshalList(res.Responses[c.tableName])
}

// GetChildren fetches all of the children of a given Node from DynamoDB.
func (c Client) GetChildren(n Node) ([]*Node, error) {
	log := c.log.Indent("GetChildren")
	log.Debug("called...")
	defer log.Debug("exited")

	// No reason to query Dynamo if the node does not have children.
	if !n.HasChildren() {
		return []*Node{}, nil
	}

	// The children of the current Node are those whose ParentID attribute are
	// equivalent to the current Node's ID.
	return c.query(n.ID, "ParentID")
}

// GetSiblings fetches all of the siblings of a given Node from DynamoDB.
func (c Client) GetSiblings(n Node) ([]*Node, error) {
	// TODO: determine if this should also return the node that was passed in
	log := c.log.Indent("GetSiblings")
	log.Debug("called...")
	defer log.Debug("exited")

	// if the node has no parent, it cannot have siblings
	if !n.HasParent() {
		return []*Node{}, nil
	}

	// The siblings of the current Node are those whose ParentID attribute are
	// equivalent to the current Node's ParentID.
	return c.query(n.ParentID, "ParentID")
}

// query is responsible for actually running a query against a DynamoDB table/GSI.
func (c Client) query(id, partitionKey string) ([]*Node, error) {
	log := c.log.Indent("query")
	log.Debug("called...")
	defer log.Debug("exited")

	log.Debug("generating QueryInput")
	input := &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": {S: aws.String(id)},
		},
		ExpressionAttributeNames: map[string]*string{
			"#pkey": aws.String(partitionKey),
		},
		KeyConditionExpression: aws.String("#pkey = :id"),
		TableName:              aws.String(c.tableName),
		IndexName:              aws.String(c.gsiName),
	}

	log.Debugf("QueryInput:\n%v", input)

	log.Debug("calling Query...")
	res, err := c.dataStore.Query(input)
	if err != nil {
		return nil, errors.Wrap(err, "query: Error retrieving data from DynamoDB")
	}

	return unmarshalList(res.Items)
}

// Put stores the given Node in DynamoDB.
func (c Client) Put(in Node) error {
	log := c.log.Indent("Put")
	log.Debug("called...")
	defer log.Debug("exited")

	log.Debug("marshalling data...")
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		return err
	}

	log.Debug("generating PutItemInput...")
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(c.tableName),
	}

	log.Debugf("PutItemInput:\n%v", input)

	log.Debug("calling PutItem...")
	if _, err := c.dataStore.PutItem(input); err != nil {
		return err
	}

	return nil
}

// BatchPut stores the given Node(s), in DynamoDB.
func (c Client) BatchPut(in []*Node) error {
	log := c.log.Indent("BatchPut")
	log.Debug("called...")
	defer log.Debug("exited")

	log.Debug("generating BatchWriteItemInput...")
	wr := []*dynamodb.WriteRequest{}

	for _, n := range in {
		av, err := dynamodbattribute.MarshalMap(n)
		if err != nil {
			return err
		}

		wr = append(wr, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: av},
		})
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			c.tableName: wr,
		},
	}

	log.Debugf("BatchWriteItemInput:\n%v", input)

	log.Debug("calling BatchWriteItem...")
	if _, err := c.dataStore.BatchWriteItem(input); err != nil {
		return err
	}

	return nil
}

// Delete removes the given Node, and all children, from DynamoDB.
func (c Client) Delete(in *Node) error {
	log := c.log.Indent("Delete")
	log.Debug("called...")
	defer log.Debug("exited")

	log.Debug("marshalling input...")
	av, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		return err
	}

	log.Debug("generating DeleteItemInput...")
	input := &dynamodb.DeleteItemInput{
		Key:       av,
		TableName: aws.String(c.tableName),
	}

	log.Debugf("DeleteItemInput:\n%v", input)

	log.Debug("calling DeleteItem...")
	if _, err := c.dataStore.DeleteItem(input); err != nil {
		return err
	}

	return nil
}

// unmarshalList unmarshalles a list of results from dynamo into a slice of Nodes.
func unmarshalList(avs []map[string]*dynamodb.AttributeValue) ([]*Node, error) {
	nodes := []*Node{}

	for _, av := range avs {
		n := &Node{}
		if err := dynamodbattribute.UnmarshalMap(av, n); err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	return nodes, nil
}
