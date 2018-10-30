package node

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Marshal does a thing.
func Marshal(n Node) ([]map[string]*dynamodb.AttributeValue, error) {
	results := []map[string]*dynamodb.AttributeValue{}

	m, err := dynamodbattribute.MarshalMap(n)
	if err != nil {
		return nil, err
	}

	results = append(results, m)

	if len(n.Children) > 0 {
		for _, c := range n.Children {
			res, err := Marshal(*c)
			if err != nil {
				return nil, err
			}

			results = append(results, res...)
		}
	}

	return results, nil
}

// Unmarshal does a thing.
func Unmarshal() {}
