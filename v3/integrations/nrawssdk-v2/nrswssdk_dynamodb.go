package nrawssdk

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	OP_READ       = "read"
	OP_CREATE     = "create"
	OP_WRITE      = "write"
	OP_UPDATE     = "update"
	OP_DELETE     = "delete"
	OP_READ_WRITE = "read_write"
)

type Query struct {
	Key                       interface{}                             `json:"key,omitempty"`
	Item                      interface{}                             `json:"item,omitempty"`
	TableName                 interface{}                             `json:"tableName,omitempty"`
	ConditionExpression       *string                                 `json:"conditionExpression,omitempty"`
	ConditionalOperator       types.ConditionalOperator               `json:"conditionalOperator,omitempty"`
	Expected                  map[string]types.ExpectedAttributeValue `json:"expected,omitempty"`
	ExpressionAttributeNames  map[string]string                       `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]types.AttributeValue         `json:"expressionAttributeValues,omitempty"`
	AttributesToGet           []string                                `json:"attributesToGet,omitempty"`
	AttributeUpdates          map[string]types.AttributeValueUpdate   `json:"attributeUpdates,omitempty"`
	Statement                 *string                                 `json:"statement,omitempty"`
	Parameters                []types.AttributeValue                  `json:"parameters,omitempty"`
	KeyConditionExpression    *string                                 `json:"keyConditionExpression,omitempty"`
	FilterExpression          *string                                 `json:"filterExpression,omitempty"`
	ProjectionExpression      *string                                 `json:"projectionExpression,omitempty"`
	QueryFilter               map[string]types.Condition              `json:"queryFilter,omitempty"`
	ScanFilter                interface{}                             `json:"scanFilter,omitempty"`
	UpdateExpression          *string                                 `json:"updateExpression,omitempty"`
}

type parameters struct {
	Payload     Query  `json:"payload"`
	PayloadType string `json:"payloadType"`
}

func handleRequest(in interface{}) (parameter []parameters) {
	if in == nil {
		return
	}
	switch input := in.(type) {
	case *dynamodb.PutItemInput:
		var query Query
		query.Item = input.Item
		query.TableName = input.TableName
		query.ConditionExpression = input.ConditionExpression
		query.ConditionalOperator = input.ConditionalOperator
		query.Expected = input.Expected
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		parameter = append(parameter, parameters{query, OP_WRITE})
		return
	case *dynamodb.GetItemInput:
		if input.ProjectionExpression == nil {
			return
		}
		var query Query
		query.Key = input.Key
		query.TableName = input.TableName
		query.AttributesToGet = input.AttributesToGet
		query.ProjectionExpression = input.ProjectionExpression
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.UpdateItemInput:
		var query Query
		query.Key = input.Key
		query.TableName = input.TableName
		query.AttributeUpdates = input.AttributeUpdates
		query.ConditionExpression = input.ConditionExpression
		query.ConditionalOperator = input.ConditionalOperator
		query.Expected = input.Expected
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		parameter = append(parameter, parameters{query, OP_UPDATE})
		return
	case *dynamodb.DeleteItemInput:
		if input.ConditionExpression == nil {
			return
		}
		var query Query
		query.Key = input.Key
		query.TableName = input.TableName
		query.ConditionExpression = input.ConditionExpression
		query.ConditionalOperator = input.ConditionalOperator
		query.Expected = input.Expected
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		parameter = append(parameter, parameters{query, OP_DELETE})
		return
	case *dynamodb.BatchGetItemInput:
		requestItems := input.RequestItems
		for k, v := range requestItems {
			var query Query
			query.Key = v.Keys
			query.TableName = k
			query.AttributesToGet = v.AttributesToGet
			query.ProjectionExpression = v.ProjectionExpression
			query.ExpressionAttributeNames = v.ExpressionAttributeNames
			parameter = append(parameter, parameters{query, OP_READ})
		}
		return
	case *dynamodb.BatchWriteItemInput:
		requestItems := input.RequestItems
		for k, v := range requestItems {
			for i := range v {
				if v[i].PutRequest != nil {
					var query Query
					query.Item = v[i].PutRequest.Item
					query.TableName = k
					parameter = append(parameter, parameters{query, OP_WRITE})
				}
				if v[i].DeleteRequest != nil {
					var query Query
					query.Key = v[i].DeleteRequest.Key
					query.TableName = k
					parameter = append(parameter, parameters{query, OP_DELETE})
				}
			}
		}
		return
	case *dynamodb.BatchExecuteStatementInput:
		requestItems := input.Statements
		for i := range requestItems {
			var query Query
			query.Statement = requestItems[i].Statement
			query.Parameters = requestItems[i].Parameters
			parameter = append(parameter, parameters{query, OP_READ_WRITE})
		}
		return
	case *dynamodb.QueryInput:
		if input.FilterExpression == nil && input.KeyConditionExpression == nil && input.ProjectionExpression == nil {
			return nil
		}
		var query Query
		query.TableName = input.TableName
		query.KeyConditionExpression = input.KeyConditionExpression
		query.FilterExpression = input.FilterExpression
		query.ProjectionExpression = input.ProjectionExpression
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		query.QueryFilter = input.QueryFilter
		query.AttributesToGet = input.AttributesToGet
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.ScanInput:
		if input.FilterExpression == nil && input.ProjectionExpression == nil {
			return nil
		}
		var query Query
		query.TableName = input.TableName
		query.FilterExpression = input.FilterExpression
		query.ProjectionExpression = input.ProjectionExpression
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		//query.ScanFilter = input.ScanFilter
		query.AttributesToGet = input.AttributesToGet
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.TransactGetItemsInput:
		requestItems := input.TransactItems

		for i := range requestItems {
			var query Query
			get := requestItems[i]
			query.TableName = get.Get.TableName
			query.Key = get.Get.Key
			query.ProjectionExpression = get.Get.ProjectionExpression
			query.ExpressionAttributeNames = get.Get.ExpressionAttributeNames
			parameter = append(parameter, parameters{query, OP_READ})
		}
		return
	case *dynamodb.TransactWriteItemsInput:
		requestItems := input.TransactItems
		for i := range requestItems {

			if requestItems[i].ConditionCheck != nil {
				conditionCheck := requestItems[i].ConditionCheck
				var query Query
				query.TableName = conditionCheck.TableName
				query.Key = conditionCheck.Key
				query.ConditionExpression = conditionCheck.ConditionExpression
				query.ExpressionAttributeNames = conditionCheck.ExpressionAttributeNames
				query.ExpressionAttributeValues = conditionCheck.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_READ})
			}
			if requestItems[i].Put != nil {
				put := requestItems[i].Put
				var query Query
				query.TableName = put.TableName
				query.Item = put.Item
				query.ConditionExpression = put.ConditionExpression
				query.ExpressionAttributeNames = put.ExpressionAttributeNames
				query.ExpressionAttributeValues = put.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_WRITE})
			}
			if requestItems[i].Update != nil {
				update := requestItems[i].Update
				var query Query
				query.TableName = update.TableName
				query.Key = update.Key
				query.ConditionExpression = update.ConditionExpression
				query.UpdateExpression = update.UpdateExpression
				query.ExpressionAttributeNames = update.ExpressionAttributeNames
				query.ExpressionAttributeValues = update.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_UPDATE})
			}
			if requestItems[i].Delete != nil {
				delete := requestItems[i].Delete
				var query Query
				query.TableName = delete.TableName
				query.Key = delete.Key
				query.ConditionExpression = delete.ConditionExpression
				query.ExpressionAttributeNames = delete.ExpressionAttributeNames
				query.ExpressionAttributeValues = delete.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_DELETE})
			}

		}
		return
	case *dynamodb.ExecuteStatementInput:
		var query Query
		query.Statement = input.Statement
		query.Parameters = input.Parameters
		parameter = append(parameter, parameters{query, OP_READ_WRITE})
		return
	case *dynamodb.ExecuteTransactionInput:

		requestItems := input.TransactStatements
		for i := range requestItems {
			var query Query
			query.Statement = requestItems[i].Statement
			query.Parameters = requestItems[i].Parameters
			parameter = append(parameter, parameters{query, OP_READ_WRITE})
		}
		return
	default:
	}
	return
}
