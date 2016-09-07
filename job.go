package bullion

import (
	"encoding/json"
	"strconv"
	"time"
)

const (
	// BullionJobBackoffNone is the value for no backoff
	BullionJobBackoffNone = "none"
	// BullionJobBackoffFixed is the value for fixed backoff
	BullionJobBackoffFixed = "fixed"
	// BullionJobBackoffExponential is the value for exponential backoff
	BullionJobBackoffExponential = "exponential"
)

// Job is the main struct for the job
type Job struct {
	ID           string
	Data         interface{}
	Opts         *JobOptions
	Progress     float64
	Delay        int64
	Timestamp    int64
	Attempts     int
	AttemptsMade int
	Backoff      string
	BackoffDelay int64
	Stacktraces  []interface{}
	ReturnValue  interface{}
	Bull         *Bullion
}

// JobOptions is the struct for job options
type JobOptions struct {
	LIFO         bool      // last in first out
	ETA          time.Time // job ETA timestamp
	Backoff      string    // backoff type: none, fixed, exponential
	BackoffDelay int       // backoff delay
	Attempts     int       // maximum attempts allowed on job
}

// AddJob adds a job into the queue
func (bull *Bullion) AddJob(job *Job) error {
	job.Bull = bull
}

// JobFromData constructs a job from the key value mapping data
func (bull *Bullion) JobFromData(id string, data map[string]string) (*Job, error) {
	var jobData interface{}
	err := json.Unmarshal([]byte(data["data"]), &jobData)
	if err != nil {
		return nil, err
	}
	var opts interface{}
	err = json.Unmarshal([]byte(data["opts"]), &opts)
	if err != nil {
		return nil, err
	}
	var delay int
	if data["delay"] == "undefined" {
		delay = 0
	} else {
		delay, _ = strconv.Atoi(data["delay"])
	}
	timestamp, _ := strconv.Atoi(data["timestamp"])
	progress, _ := strconv.ParseFloat(data["progress"], 64)

}

// GetLockKey returns the key of the lock on the job in redis
func (job *Job) GetLockKey() string {
	key := job.Bull.GetKeyPrefix() + job.ID + ":lock"
	return key
}

// Serialize returns a key value mapping of the job
func (job *Job) Serialize() (map[string]string, error) {
	data, err := json.Marshal(job.Data)
	if err != nil {
		return nil, err
	}
	timestamp := strconv.FormatInt(job.Timestamp, 10)
	delay := strconv.FormatInt(job.Delay, 10)
	opts := map[string]interface{}{
		"delay": delay,
	}
	if job.Backoff == BullionJobBackoffFixed || job.Backoff == BullionJobBackoffExponential {
		opts["backoff"] = map[string]interface{}{
			"type":  job.Backoff,
			"delay": job.BackoffDelay,
		}
	}
	_opts, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	stacktraces, err := json.Marshal(job.Stacktraces)
	if err != nil {
		return nil, err
	}
	returnValue, err := json.Marshal(job.ReturnValue)
	if err != nil {
		return nil, err
	}
	_job := map[string]string{
		"data":         string(data),
		"opts":         string(_opts),
		"delay":        delay,
		"timestamp":    timestamp,
		"attempts":     strconv.Itoa(job.Attempts),
		"attemptsMade": strconv.Itoa(job.AttemptsMade),
		"progress":     strconv.FormatFloat(job.Progress, 'f', -1, 64),
		"stacktrace":   string(stacktraces),
		"returnvalue":  string(returnValue),
	}
	return _job, nil
}
