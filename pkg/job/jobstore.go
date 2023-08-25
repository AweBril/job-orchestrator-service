package job

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.comcast.com/mesa/mafslog/ecslog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DDBJobStorerIDField      = "JobId"
	DDBJobStorerGroupIDField = "GroupId"
	DDBJobStorerJobJSONField = "JobJSON"
	DDBJobStorerTTLField     = "TTL"
)

const DoneJobTTL = 14 * 24 * time.Hour // 2 weeks

// JobStorer represents an external storage component for storing Jobs to be rescheduled if the orchestrator crashes.
type JobStorer interface {
	GetJobs(logger *ecslog.Logger, opts GetJobsOptions) ([]*Job, error)
	GetJob(id string) (*Job, error)
	UpdateJob(job *Job) error
	DeleteJob(id string) error
}

// SortOption is an option to sort job results based on a field, and which direction to sort in.
type SortOption struct {
	Field string `json:"field"`
	Asc   bool   `json:"asc"`
}

// SortOptions is a list of sort options.
type SortOptions []SortOption

// Sort sorts the jobs list according to the sort options.
func (o SortOptions) Sort(jobs []*Job) []*Job {
	if len(o) == 0 {
		return jobs
	}

	sort.Slice(jobs, func(i, j int) bool {
		for _, opt := range o {
			switch opt.Field {
			case "id":
				a := jobs[i].ID
				b := jobs[j].ID
				if a < b {
					return opt.Asc
				}
				if a > b {
					return !opt.Asc
				}
			case "startTime":
				a := jobs[i].StartTime
				b := jobs[j].StartTime
				if a-b < 0 {
					return opt.Asc
				}
				if a-b > 0 {
					return !opt.Asc
				}
			}
		}
		return false
	})

	return jobs
}

// MongoFindOptions returns the findOptions for sorting mongo documents.
func (o SortOptions) MongoFindOptions() *options.FindOptions {
	doc := bson.D{}
	for _, sortOpt := range o {
		doc = append(doc, bson.E{
			Key:   sortOpt.Field,
			Value: sortDirectionBoolToInt(sortOpt.Asc),
		})
	}
	return options.Find().SetSort(doc)
}

// NumberFilter is a filter for a value to fit between its gt and lt fields, if that field exists.
type NumberFilter struct {
	Gt *int64 `json:"gt,omitempty"`
	Lt *int64 `json:"lt,omitempty"`
}

// Filter filters the given int64 based on if it is greater than the .Gt field and less than the .Lt field, if they exist.
func (f NumberFilter) Filter(in int64) bool {
	if f.Gt != nil && in < *f.Gt {
		return false
	}
	if f.Lt != nil && in > *f.Lt {
		return false
	}

	return true
}

// ComparableFilter is a filter for if the value is equal to any in the Eq list.
type ComparableFilter[T comparable] struct {
	Eq []T `json:"eq"`
}

// Filter filters the given comparable for if its value is equal to any in the .Eq list.
func (f ComparableFilter[T]) Filter(n T) bool {
	for _, eq := range f.Eq {
		if n == eq {
			return true
		}
	}
	return false
}

// SeachFilter is a filter for if any of the given values is like the Like string.
type SearchFilter struct {
	Like string `json:"like"`
}

// Filter filters for if any of the given words are like the .Like field.
func (f SearchFilter) Filter(words ...string) bool {
	for _, word := range words {
		if strings.Contains(word, f.Like) {
			return true
		}
	}
	return false
}

// FilterOptions is the complete list of filter options that can be applied to jobs. If the field is nil, no filter is applied.
type FilterOptions struct {
	StartTime *NumberFilter                        `json:"startTime,omitempty"`
	Statuses  *ComparableFilter[JobStatusCategory] `json:"statuses,omitempty"`
	Search    *SearchFilter                        `json:"search,omitempty"`
}

// Filter filters the job by all of the filters in the filter object.
func (f FilterOptions) Filter(j *Job) bool {
	if f.StartTime != nil && !f.StartTime.Filter(j.StartTime.Int64()) {
		return false
	}
	if f.Statuses != nil && !f.Statuses.Filter(j.Status.Category()) {
		return false
	}
	if f.Search != nil && !f.Search.Filter(j.ID) {
		return false
	}

	return true
}

// MongoFilter is used to filter mongo documents and returns the query for filtering.
func (f FilterOptions) MongoFilter() (or, and []bson.M) {
	statusQuery := []bson.M{}
	if f.Statuses != nil {
		for _, statusCategory := range f.Statuses.Eq {
			for _, status := range statusCategory.Statuses() {
				statusQuery = append(statusQuery, bson.M{"job.status": string(status)})
			}
		}
	}

	rangeAndSearchQuery := []bson.M{}
	if f.StartTime != nil {
		rangeQuery := bson.M{}
		if f.StartTime.Gt != nil {
			rangeQuery = bson.M{"$gte": f.StartTime.Gt}
		}
		if f.StartTime.Lt != nil {
			rangeQuery["$lte"] = f.StartTime.Lt
		}
		rangeAndSearchQuery = append(rangeAndSearchQuery, bson.M{"job.jobrequest.starttime": rangeQuery})
	}

	if f.Search != nil {
		rangeAndSearchQuery = append(rangeAndSearchQuery, bson.M{"job.jobrequest.id": bson.M{"$regex": primitive.Regex{
			Pattern: f.Search.Like,
			Options: "i",
		}}})
	}

	return statusQuery, rangeAndSearchQuery
}

// GetJobsOptions is the list of options applied to getting jobs from the storer- any retrieved job is filtered by
// the filters first, then after all jobs are retrieved, they are sorted based on the sort options.
type GetJobsOptions struct {
	Sorts   SortOptions   `json:"sorts"`
	Filters FilterOptions `json:"filters"`
}

// stringify converts any given argument into a json string.
func stringify(data any) string {
	buf := &bytes.Buffer{}
	json.NewEncoder(buf).Encode(data)
	return buf.String()
}
