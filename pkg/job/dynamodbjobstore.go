package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.comcast.com/mesa/mafslog/ecslog"
)

// DynamoDBJobStorer uses DynamoDB to store jobs externally to be rescheduled if the orchestrator ever crashes.
type DynamoDBJobStorer struct {
	ddb       *dynamodb.DynamoDB
	tableName string
	groupId   string
}

// NewDynamoDBJobStorer creates a new DynamoDBJobStorer using the given table, or creating it if it does not exist. Entries that
// are done will fall of the table with a 2 week TTL.
func NewDynamoDBJobStorer(sess *session.Session, tableName, groupId string) (*DynamoDBJobStorer, error) {
	ddb := dynamodb.New(sess)

	_, err := ddb.DescribeTable(&dynamodb.DescribeTableInput{TableName: &tableName})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			_, err = ddb.CreateTable(&dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String(DDBJobStorerGroupIDField),
						AttributeType: aws.String("S"),
					},
					{
						AttributeName: aws.String(DDBJobStorerIDField),
						AttributeType: aws.String("S"),
					},
				},
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String(DDBJobStorerGroupIDField),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String(DDBJobStorerIDField),
						KeyType:       aws.String("RANGE"),
					},
				},
				BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
				TableName:   aws.String(tableName),
			})
			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() != dynamodb.ErrCodeResourceInUseException {
					return nil, fmt.Errorf("failed creating DynamoDB table (%v): %v", tableName, err)
				}
			}
			err = ddb.WaitUntilTableExists(&dynamodb.DescribeTableInput{TableName: &tableName})
			if err != nil {
				return nil, fmt.Errorf("failed waiting for DynamoDB table to be ready (%v): %v", tableName, err)
			}
			_, err = ddb.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
				TableName: &tableName,
				TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
					AttributeName: aws.String(DDBJobStorerTTLField),
					Enabled:       aws.Bool(true),
				},
			})
			if err != nil {
				return nil, fmt.Errorf("failed updating DynamoDB table TTL field (%v): %v", tableName, err)
			}

		} else {
			return nil, fmt.Errorf("failed to describe ddb table: %w", err)
		}
	}

	return &DynamoDBJobStorer{
		ddb:       dynamodb.New(sess),
		tableName: tableName,
		groupId:   groupId,
	}, nil
}

// GetJobs gets the jobs that satisfy the filters in the given options, sorted by the sorts in the options.
func (js *DynamoDBJobStorer) GetJobs(logger *ecslog.Logger, opts GetJobsOptions) ([]*Job, error) {
	jobs := []*Job{}
	errMap := map[string]string{}
	err := js.ddb.QueryPages(&dynamodb.QueryInput{
		TableName:              &js.tableName,
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :groupId", DDBJobStorerGroupIDField)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":groupId": {S: &js.groupId},
		},
	}, func(page *dynamodb.QueryOutput, lastPage bool) bool {
		for _, dynamoData := range page.Items {
			job := &Job{}
			err := json.NewDecoder(bytes.NewBufferString(*dynamoData[DDBJobStorerJobJSONField].S)).Decode(job)
			if err != nil {
				errMap["err"] = err.Error()
				return true
			}
			if opts.Filters.Filter(job) {
				jobs = append(jobs, job)
			}
		}
		return !lastPage
	})
	if err != nil {
		return []*Job{}, fmt.Errorf("error while querying dynamoDB table %s: %v", js.tableName, err)
	}
	if len(errMap) > 0 {
		return []*Job{}, fmt.Errorf("found %v errors when decoding from DynamoDB: %v", len(errMap), stringify(errMap))
	}
	return opts.Sorts.Sort(jobs), nil
}

// GetJob gets the job from DynamoDB. If it doesn't exist, no error is thrown, but job is nil.
func (js *DynamoDBJobStorer) GetJob(id string) (*Job, error) {
	output, err := js.ddb.GetItem(&dynamodb.GetItemInput{
		TableName: &js.tableName,
		Key: map[string]*dynamodb.AttributeValue{
			DDBJobStorerGroupIDField: {S: &js.groupId},
			DDBJobStorerIDField:      {S: &id},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if output.Item == nil {
		return nil, nil
	}
	job := &Job{}
	err = json.NewDecoder(bytes.NewBufferString(*output.Item[DDBJobStorerJobJSONField].S)).Decode(job)
	return job, err
}

// UpdateJob adds the given job to the remote store in DynamoDB, or updates it if it already exists.
func (js *DynamoDBJobStorer) UpdateJob(j *Job) error {
	ttl := "0"
	if j.GetStatus().Category() == StatusCategoryDone {
		ttl = fmt.Sprintf("%d", time.Now().Add(DoneJobTTL).Unix())
	}

	_, err := js.ddb.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			DDBJobStorerGroupIDField: {S: &js.groupId},
			DDBJobStorerIDField:      {S: &j.ID},
			DDBJobStorerJobJSONField: {S: aws.String(stringify(j))},
			DDBJobStorerTTLField:     {N: &ttl},
		},
		TableName: &js.tableName,
	})
	if err != nil {
		return fmt.Errorf("failed to add or update job in DynamoDB: %w", err)
	}
	return nil
}

// DeleteJob deletes the job from the remote store in DynamoDB.
func (js *DynamoDBJobStorer) DeleteJob(id string) error {
	_, err := js.ddb.DeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			DDBJobStorerGroupIDField: {S: &js.groupId},
			DDBJobStorerIDField:      {S: &id},
		},
		TableName: &js.tableName,
	})
	if err == nil {
		return fmt.Errorf("failed to delete job (%v) from DynamoDB: %v", id, err)
	}
	return nil
}
