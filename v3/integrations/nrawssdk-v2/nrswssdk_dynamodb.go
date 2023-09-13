package nrawssdk

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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
	Key                       interface{}       `json:"key,omitempty"`
	Item                      interface{}       `json:"item,omitempty"`
	TableName                 string            `json:"tableName,omitempty"`
	ConditionExpression       string            `json:"conditionExpression,omitempty"`
	ConditionalOperator       interface{}       `json:"conditionalOperator,omitempty"`
	Expected                  interface{}       `json:"expected,omitempty"`
	ExpressionAttributeNames  map[string]string `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues interface{}       `json:"expressionAttributeValues,omitempty"`
	AttributesToGet           []string          `json:"attributesToGet,omitempty"`
	AttributeUpdates          []string          `json:"attributeUpdates,omitempty"`
	Statement                 string            `json:"statement,omitempty"`
	Parameters                interface{}       `json:"parameters,omitempty"`
	KeyConditionExpression    string            `json:"keyConditionExpression,omitempty"`
	FilterExpression          string            `json:"filterExpression,omitempty"`
	ProjectionExpression      string            `json:"projectionExpression,omitempty"`
	QueryFilter               string            `json:"queryFilter,omitempty"`
	ScanFilter                string            `json:"scanFilter,omitempty"`
	UpdateExpression          string            `json:"updateExpression,omitempty"`
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
		query.Item = UnmarshalMap(input.Item)
		query.TableName = GetPointerValue(input.TableName)
		query.ConditionExpression = GetPointerValue(input.ConditionExpression)
		query.ConditionalOperator = input.ConditionalOperator
		//query.Expected = UnmarshalMap(input.Expected) // check this how we can do this
		query.ConditionalOperator = input.ConditionalOperator
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = UnmarshalMap(input.ExpressionAttributeValues)
		parameter = append(parameter, parameters{query, OP_WRITE})
		return
	case *dynamodb.GetItemInput:
		var query Query
		query.Key = UnmarshalMap(input.Key)
		query.TableName = GetPointerValue(input.TableName)
		query.AttributesToGet = input.AttributesToGet
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.UpdateItemInput:
		var query Query
		query.Key = UnmarshalMap(input.Key)
		query.TableName = GetPointerValue(input.TableName)
		//query.AttributeUpdates = input.AttributeUpdates
		query.ConditionExpression = GetPointerValue(input.ConditionExpression)
		query.ConditionalOperator = input.ConditionalOperator
		//query.Expected = UnmarshalMap(input.Expected) // check this how we can do this
		query.ConditionalOperator = input.ConditionalOperator
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = UnmarshalMap(input.ExpressionAttributeValues)
		parameter = append(parameter, parameters{query, OP_UPDATE})
		return
	case *dynamodb.DeleteItemInput:
		var query Query
		query.Key = UnmarshalMap(input.Key)
		query.TableName = GetPointerValue(input.TableName)
		query.ConditionExpression = GetPointerValue(input.ConditionExpression)
		query.ConditionalOperator = input.ConditionalOperator
		//query.Expected = UnmarshalMap(input.Expected) // check this how we can do this
		query.ConditionalOperator = input.ConditionalOperator
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = UnmarshalMap(input.ExpressionAttributeValues)
		parameter = append(parameter, parameters{query, OP_DELETE})
		return
	case *dynamodb.BatchGetItemInput:
		requestItems := input.RequestItems
		for k, v := range requestItems {
			var query Query
			query.Key = UnmarshalListMap(v.Keys)
			query.TableName = k
			query.AttributesToGet = v.AttributesToGet
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
					query.Item = UnmarshalMap(v[i].PutRequest.Item)
					query.TableName = k
					parameter = append(parameter, parameters{query, OP_WRITE})
				}
				if v[i].DeleteRequest != nil {
					var query Query
					query.Key = UnmarshalMap(v[i].DeleteRequest.Key)
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
			query.Statement = *requestItems[i].Statement
			query.Parameters = UnmarshalList(requestItems[i].Parameters)
			parameter = append(parameter, parameters{query, OP_READ_WRITE})
		}
		return
	case *dynamodb.QueryInput:
		var query Query
		query.TableName = GetPointerValue(input.TableName)
		query.KeyConditionExpression = GetPointerValue(input.KeyConditionExpression)
		query.FilterExpression = GetPointerValue(input.FilterExpression)
		query.ProjectionExpression = GetPointerValue(input.ProjectionExpression)
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = UnmarshalMap(input.ExpressionAttributeValues)
		//query.QueryFilter = input.QueryFilter
		query.AttributesToGet = input.AttributesToGet
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.ScanInput:

		// query.setScanFilter(request.ScanFilter());

		var query Query
		query.TableName = GetPointerValue(input.TableName)
		query.FilterExpression = GetPointerValue(input.FilterExpression)
		query.ProjectionExpression = GetPointerValue(input.ProjectionExpression)
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = UnmarshalMap(input.ExpressionAttributeValues)
		//query.ScanFilter = input.ScanFilter
		query.AttributesToGet = input.AttributesToGet
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.TransactGetItemsInput:
		requestItems := input.TransactItems

		for i := range requestItems {
			var query Query
			get := requestItems[i]
			query.TableName = GetPointerValue(get.Get.TableName)
			query.Key = get.Get.Key
			query.ProjectionExpression = GetPointerValue(get.Get.ProjectionExpression)
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
				query.TableName = GetPointerValue(conditionCheck.TableName)
				query.Key = UnmarshalMap(conditionCheck.Key)
				query.ConditionExpression = GetPointerValue(conditionCheck.ConditionExpression)
				query.ExpressionAttributeNames = conditionCheck.ExpressionAttributeNames
				query.ExpressionAttributeValues = UnmarshalMap(conditionCheck.ExpressionAttributeValues)
				parameter = append(parameter, parameters{query, OP_READ})
			}
			if requestItems[i].Put != nil {
				put := requestItems[i].Put
				var query Query
				query.TableName = GetPointerValue(put.TableName)
				query.Item = UnmarshalMap(put.Item)
				query.ConditionExpression = GetPointerValue(put.ConditionExpression)
				query.ExpressionAttributeNames = put.ExpressionAttributeNames
				query.ExpressionAttributeValues = UnmarshalMap(put.ExpressionAttributeValues)
				parameter = append(parameter, parameters{query, OP_WRITE})
			}
			if requestItems[i].Update != nil {
				update := requestItems[i].Update
				var query Query
				query.TableName = GetPointerValue(update.TableName)
				query.Key = UnmarshalMap(update.Key)
				query.ConditionExpression = GetPointerValue(update.ConditionExpression)
				query.UpdateExpression = GetPointerValue(update.UpdateExpression)
				query.ExpressionAttributeNames = update.ExpressionAttributeNames
				query.ExpressionAttributeValues = UnmarshalMap(update.ExpressionAttributeValues)
				parameter = append(parameter, parameters{query, OP_UPDATE})
			}
			if requestItems[i].Delete != nil {
				delete := requestItems[i].Delete
				var query Query
				query.TableName = GetPointerValue(delete.TableName)
				query.Key = UnmarshalMap(delete.Key)
				query.ConditionExpression = GetPointerValue(delete.ConditionExpression)
				query.ExpressionAttributeNames = delete.ExpressionAttributeNames
				query.ExpressionAttributeValues = UnmarshalMap(delete.ExpressionAttributeValues)
				parameter = append(parameter, parameters{query, OP_DELETE})
			}

		}
		return
	case *dynamodb.ExecuteStatementInput:
		var query Query
		query.Statement = GetPointerValue(input.Statement)
		query.Parameters = input.Parameters
		parameter = append(parameter, parameters{query, OP_READ_WRITE})
		return
	case *dynamodb.ExecuteTransactionInput:

		requestItems := input.TransactStatements
		for i := range requestItems {
			var query Query
			query.Statement = GetPointerValue(requestItems[i].Statement)
			query.Parameters = UnmarshalList(requestItems[i].Parameters)
			parameter = append(parameter, parameters{query, OP_READ_WRITE})
		}
		return
	default:
	}
	return
}

func UnmarshalMap(att map[string]types.AttributeValue) map[string]interface{} {
	Item := map[string]interface{}{}
	attributevalue.UnmarshalMap(att, &Item)
	return Item
}

func UnmarshalListMap(att []map[string]types.AttributeValue) []map[string]interface{} {
	Items := []map[string]interface{}{}
	for i := range att {
		Item := map[string]interface{}{}
		attributevalue.UnmarshalMap(att[i], &Item)
		Items = append(Items, Item)
	}
	return Items
}

func UnmarshalList(att []types.AttributeValue) []interface{} {
	Item := []interface{}{}
	attributevalue.UnmarshalList(att, &Item)
	return Item
}

func GetPointerValue(in *string) string {
	if in == nil {
		return ""
	} else {
		return *in
	}
}
