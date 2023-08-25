package job

import (
	"context"
	"fmt"
	"time"

	"github.comcast.com/mesa/mafaas-common/constants"
	"github.comcast.com/mesa/mafaas-common/httpresponse"
	"github.comcast.com/mesa/mafaas-types/db"
	"github.comcast.com/mesa/mafslog/ecslog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
)

var DEFAULT_MONGO_TIMEOUT = 30 * time.Second

// MongoJobEntry represents a document entry for orchestrator in a collection holding the job details.
type MongoJobEntry struct {
	GroupId string `json:"groupId"`
	Job
	CreatedAt time.Time  `json:"createdAt" bson:"created_at"`
	ExpireAt  *time.Time `json:"expireAt,omitempty" bson:"expire_at,omitempty"`
}

// MongoJobStorer uses MongoDB to store jobs externally to be rescheduled if the orchestrator ever crashes.
type MongoJobStorer struct {
	collection   *mongo.Collection
	mongoTimeout time.Duration
	groupId      string
	dbRetries    int
}

// newMongoClient establishes a connection to the mongodb.
func newMongoClient(uri string, timeOut time.Duration) (*mongo.Client, *db.ErrorResponse) {
	clientOptions := &options.ClientOptions{}
	clientOptions.ApplyURI(uri).SetMonitor(otelmongo.NewMonitor())

	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, db.BuildDbError(httpresponse.MONGO_CLIENT_ERROR, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, db.BuildDbError(httpresponse.MONGO_CONNECTION_ERROR, err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, db.BuildDbError(httpresponse.MONGO_PING_ERROR, err)
	}

	return client, nil
}

// NewMongoJobStorer creates a new MongoJobStorer using the given collection, or creating it if it does not exist. Entries that
// are done will fall off the collection with a 1 week TTL.
func NewMongoJobStorer(uri, dbName string, timeOut time.Duration, collName, groupId string, mongoDbRetries int) (*MongoJobStorer, error) {
	var jobsCollection *mongo.Collection

	jobMongoClient, dbErr := newMongoClient(uri, timeOut)
	if dbErr != nil {
		return nil, fmt.Errorf("error starting mongoDB connection: %v", dbErr)
	}

	res, err := jobMongoClient.ListDatabaseNames(context.Background(), bson.M{"name": dbName})
	if err != nil {
		return nil, fmt.Errorf("failed querying mongo databases: %v", err)
	}
	if len(res) != 1 || res[0] != dbName {
		return nil, fmt.Errorf("database %s not in mongo", dbName)
	}

	collList, err := jobMongoClient.Database(dbName).ListCollectionNames(context.Background(), bson.M{"name": collName})
	if err != nil {
		return nil, fmt.Errorf("error listing collections in mongoDB collection: %v", err)
	}

	collExists := len(collList) > 0 && collList[0] == collName

	if !collExists {
		err = jobMongoClient.Database(dbName).CreateCollection(context.Background(), collName)
		if err != nil {
			return nil, fmt.Errorf("error creating orchestrator jobs collection in mongoDB:  %v", err)
		}

		jobsCollection = jobMongoClient.Database(dbName).Collection(collName)

		ttlIndex := mongo.IndexModel{
			Keys:    bsonx.Doc{{Key: "expire_at", Value: bsonx.Int32(1)}},
			Options: options.Index().SetExpireAfterSeconds(int32(0)),
		}

		_, err := jobsCollection.Indexes().CreateOne(context.Background(), ttlIndex)
		if err != nil {
			return nil, fmt.Errorf("error creating TTL index for mongoDB collection: %v", err)
		}
	} else {
		jobsCollection = jobMongoClient.Database(dbName).Collection(collName)
		ecslog.Info(fmt.Sprintf("Collection %s Exists", collName))
	}

	if mongoDbRetries > constants.JOBS_DB_MAX_RETRIES {
		mongoDbRetries = constants.JOBS_DB_MAX_RETRIES
	}

	return &MongoJobStorer{
		collection:   jobsCollection,
		mongoTimeout: timeOut,
		groupId:      groupId,
		dbRetries:    mongoDbRetries,
	}, nil
}

// sortDirectionBoolToInt converts boolean to integer value for direction to sort.
func sortDirectionBoolToInt(order bool) int {
	if order {
		return 1
	}
	return -1
}

// GetJobs gets all the jobs from mongoDB applied with filtering. If no job exist, empty job array is returned.
func (js *MongoJobStorer) GetJobs(logger *ecslog.Logger, opts GetJobsOptions) ([]*Job, error) {
	jobs := []*Job{}

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), js.mongoTimeout)
	defer cancel()

	query := bson.M{"groupid": js.groupId}

	// To filter querying the documents with array of status
	orQuery, andQuery := opts.Filters.MongoFilter()
	if len(orQuery) != 0 {
		query["$or"] = orQuery
	}
	if len(andQuery) != 0 {
		query["$and"] = andQuery
	}

	cursor, err := js.collection.Find(ctxWithTimeout, query, opts.Sorts.MongoFindOptions())
	if err != nil {
		return jobs, fmt.Errorf("error in query find in JobsDb %v", err)
	}

	defer cursor.Close(ctxWithTimeout)

	for cursor.Next(ctxWithTimeout) {
		var mongoJobEntry MongoJobEntry
		err := cursor.Decode(&mongoJobEntry)
		jobs = append(jobs, &mongoJobEntry.Job)
		if err != nil {
			logger.Error("failed decoding job from mongodb", httpresponse.MARSHAL_ERROR, err)
			continue
		}

	}
	return jobs, nil
}

// jobIdFilter is to build a filter for retreiving a job document from mongodb.
func (js *MongoJobStorer) jobIdFilter(jobId string) bson.M {
	return bson.M{"job.jobrequest.id": jobId, "groupid": js.groupId}
}

// GetJob gets the job from mongoDB. If it doesn't exist, no error is thrown, but job is nil.
func (js *MongoJobStorer) GetJob(id string) (*Job, error) {
	job := &Job{}

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), js.mongoTimeout)
	defer cancel()

	res := js.collection.FindOne(ctxWithTimeout, js.jobIdFilter(id))
	err := res.Err()
	if err == mongo.ErrNoDocuments {
		return job, err
	}
	if err != nil {
		return job, err
	}

	var mongoJobEntry MongoJobEntry
	err = res.Decode(&mongoJobEntry)
	if err != nil {
		return job, err
	}

	return &mongoJobEntry.Job, nil
}

// jobExists checks whether a job exists for the given jobId in mongoDB and returns a boolean
func (js *MongoJobStorer) jobExists(id string) (bool, error) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), js.mongoTimeout)
	defer cancel()

	res := js.collection.FindOne(ctxWithTimeout, js.jobIdFilter(id))
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return false, nil
		} else {
			return false, res.Err()
		}
	}

	return true, nil
}

// UpdateJob adds the given job to the remote store in mongoDB, or updates it if it already exists.
func (js *MongoJobStorer) UpdateJob(j *Job) error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), js.mongoTimeout)
	defer cancel()

	mongoJobEntry := &MongoJobEntry{
		GroupId: js.groupId,
		Job:     *j,
	}

	// Set ExpireAt when document is ready to set to expire, only if job is completed and status is done.
	if j.GetStatus().Category() == StatusCategoryDone {
		expireAt := time.Now().Add(time.Second * 604800).UTC() // Will be removed after 1 week=604800s
		mongoJobEntry.ExpireAt = &expireAt
	}

	var mongoDoc bson.M
	updateData, updateDataErr := bson.Marshal(mongoJobEntry)
	if updateDataErr != nil {
		return updateDataErr
	}

	updateDataErr = bson.Unmarshal(updateData, &mongoDoc)
	if updateDataErr != nil {
		return updateDataErr
	}

	update := bson.M{
		"$set": mongoDoc,
	}

	upsert := true
	opts := options.UpdateOptions{
		Upsert: &upsert,
	}

	for i := 0; i <= js.dbRetries; i++ {
		_, err := js.collection.UpdateOne(ctxWithTimeout, js.jobIdFilter(j.ID), update, &opts)
		if err == nil {
			break
		}

		// don't want to retry on this error
		if err == mongo.ErrNoDocuments {
			return err
		}

		if i == js.dbRetries {
			return err
		}
	}

	return nil
}

// DeleteJob deletes the job from the remote store in mongoDB.
func (js *MongoJobStorer) DeleteJob(id string) error {
	if ok, err := js.jobExists(id); err != nil {
		return db.BuildDbError(httpresponse.MONGO_UNKNOWN_ERROR, err).Error
	} else if !ok {
		return fmt.Errorf("delete operation failed: No job exists for the given job id %s", id)
	}

	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), js.mongoTimeout)
	defer cancel()

	for i := 0; i <= js.dbRetries; i++ {
		_, err := js.collection.DeleteOne(ctxWithTimeout, js.jobIdFilter(id))
		if err == nil {
			break
		}

		if i == js.dbRetries {
			return err
		}
	}
	return nil
}
