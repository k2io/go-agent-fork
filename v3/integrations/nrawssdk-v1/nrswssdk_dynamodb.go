package nrawssdk

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/aws/aws-sdk-go/service/dynamodb/types"
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
	Key                       interface{}                              `json:"key,omitempty"`
	Item                      interface{}                              `json:"item,omitempty"`
	TableName                 interface{}                              `json:"tableName,omitempty"`
	ConditionExpression       *string                                  `json:"conditionExpression,omitempty"`
	ConditionalOperator       *string                                  `json:"conditionalOperator,omitempty"`
	Expected                  map[string]*types.ExpectedAttributeValue `json:"expected,omitempty"`
	ExpressionAttributeNames  map[string]*string                       `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]*types.AttributeValue         `json:"expressionAttributeValues,omitempty"`
	AttributesToGet           []*string                                `json:"attributesToGet,omitempty"`
	AttributeUpdates          map[string]*types.AttributeValueUpdate   `json:"attributeUpdates,omitempty"`
	KeyConditionExpression    *string                                  `json:"keyConditionExpression,omitempty"`
	FilterExpression          *string                                  `json:"filterExpression,omitempty"`
	ProjectionExpression      *string                                  `json:"projectionExpression,omitempty"`
	QueryFilter               map[string]*types.Condition              `json:"queryFilter,omitempty"`
	ScanFilter                map[string]*types.Condition              `json:"scanFilter,omitempty"`
	UpdateExpression          *string                                  `json:"updateExpression,omitempty"`
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
		query.Item = removeNulls(input.Item)
		query.TableName = input.TableName
		query.ConditionExpression = input.ConditionExpression
		query.ConditionalOperator = input.ConditionalOperator
		query.Expected = input.Expected
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		parameter = append(parameter, parameters{query, OP_WRITE})
		return
	case *dynamodb.GetItemInput:
		var query Query
		if query.ProjectionExpression == nil {
			return
		}
		query.Key = removeNulls(input.Key)
		query.TableName = input.TableName
		query.AttributesToGet = input.AttributesToGet
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ProjectionExpression = input.ProjectionExpression
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.UpdateItemInput:
		var query Query
		query.Key = removeNulls(input.Key)
		query.TableName = input.TableName
		query.AttributeUpdates = input.AttributeUpdates
		query.ConditionExpression = input.ConditionExpression
		query.Expected = input.Expected
		query.ConditionalOperator = input.ConditionalOperator
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		query.UpdateExpression = input.UpdateExpression
		parameter = append(parameter, parameters{query, OP_UPDATE})
		return
	case *dynamodb.DeleteItemInput:

		if input.ConditionExpression == nil {
			return
		}
		var query Query
		query.Key = removeNulls(input.Key)
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
			var key []map[string]interface{}
			if v.Keys != nil {
				for i := range v.Keys {
					key = append(key, removeNulls(v.Keys[i]))
				}
			}
			query.Key = key
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
					query.Item = removeNulls(v[i].PutRequest.Item)
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
	case *dynamodb.QueryInput:

		if input.FilterExpression == nil && input.KeyConditionExpression == nil && input.ProjectionExpression == nil {
			return
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
			return
		}
		var query Query
		query.TableName = input.TableName
		query.FilterExpression = input.FilterExpression
		query.ProjectionExpression = input.ProjectionExpression
		query.ExpressionAttributeNames = input.ExpressionAttributeNames
		query.ExpressionAttributeValues = input.ExpressionAttributeValues
		query.ScanFilter = input.ScanFilter
		query.AttributesToGet = input.AttributesToGet
		parameter = append(parameter, parameters{query, OP_READ})
		return
	case *dynamodb.TransactGetItemsInput:
		requestItems := input.TransactItems

		for i := range requestItems {
			var query Query
			get := requestItems[i]
			query.TableName = get.Get.TableName
			query.Key = removeNulls(get.Get.Key)
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
				query.Key = removeNulls(conditionCheck.Key)
				query.ConditionExpression = conditionCheck.ConditionExpression
				query.ExpressionAttributeNames = conditionCheck.ExpressionAttributeNames
				query.ExpressionAttributeValues = conditionCheck.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_READ})
			}
			if requestItems[i].Put != nil {
				put := requestItems[i].Put
				var query Query
				query.TableName = put.TableName
				query.Item = removeNulls(put.Item)
				query.ConditionExpression = put.ConditionExpression
				query.ExpressionAttributeNames = put.ExpressionAttributeNames
				query.ExpressionAttributeValues = put.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_WRITE})
			}
			if requestItems[i].Update != nil {
				update := requestItems[i].Update
				var query Query
				query.TableName = update.TableName
				query.Key = removeNulls(update.Key)
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
				query.Key = removeNulls(delete.Key)
				query.ConditionExpression = delete.ConditionExpression
				query.ExpressionAttributeNames = delete.ExpressionAttributeNames
				query.ExpressionAttributeValues = delete.ExpressionAttributeValues
				parameter = append(parameter, parameters{query, OP_DELETE})
			}

		}
		return
	default:
	}
	return
}

func removeNulls(mapa map[string]*dynamodb.AttributeValue) map[string]interface{} {
	var res = make(map[string]interface{})
	for k, v := range mapa {
		res[k] = getValue(v)
	}
	return res
}

func getValue(v *dynamodb.AttributeValue) interface{} {

	type value struct {
		Value interface{} `json:"value,omitempty"`
	}
	if v.B != nil {
		return value{v.B}
	} else if v.BOOL != nil {
		return value{v.BOOL}
	} else if v.BS != nil {
		return value{v.BS}
	} else if v.L != nil {
		res := []interface{}{}
		for i := range v.L {
			res = append(res, getValue(v.L[i]))
		}
		return res
	} else if v.M != nil {
		return value{removeNulls(v.M)}
	} else if v.N != nil {
		return value{v.N}
	} else if v.NS != nil {
		return value{v.NS}
	} else if v.S != nil {
		return value{v.S}
	} else if v.SS != nil {
		return value{v.SS}
	} else {
		return nil
	}
}
