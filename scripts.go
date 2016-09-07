package bullion

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

/*
DoAddJob adds a job to the queue via lua script

Frist part of the script, we record the job data

```lua
// Increment the global counter, and use it as job id
local jobId = redis.call("INCR", #{KEY_COUNTER})
// Use HMSET to store the job data, mapped to its key
redis.call("HMSET", #{KEY_PREFIX} .. jobId, #{JOB_FIELD_1}, #{JOB_VALUE_1}, ...)
```

Second part depends on the nature of the job. If a job scheduled for
immediate execution, then it's to be added to the wait list directly:

```lua
// Use LPUSH (or RPUSH if LIFO) to push job to wait list
redis.call("LPUSH", #{KEY_QUEUE_WAIT}, jobId)
// Publish an event to signal the job is added
redis.call("PUBLISH", #{KEY_JOBS}, jobId)
```

Otherwise, it should be added to the delayed set instead:

```lua
// Calculate delay score
local timestamp = tonumber(#{ETA_TIMESTAMP}) * 0x1000 + bit.band(jobId, 0xfff)
redis.call("ZADD", #{KEY_QUEUE_DELAYED}, timestamp, jobId)
// Publish an event to signal the job is added and the delayed amount of the job
redis.call("PUBLISH", #{KEY_QUEUE_DELAYED}, (timestamp / 0x1000))
```

Finally, the job id should

Note: LUA uses 1-based array index
*/
func DoAddJob(job *Job, lifo bool) (string, error) {
	bull := job.Bull
	// Prepare the keys and argvs
	keys := []string{
		bull.GetKey(BullionKeyJobs),
		bull.GetKey(BullionKeyWait),
		bull.GetKey(BullionKeyDelayed),
		bull.GetKey(BullionKeyJobsCount),
	}
	prefix := bull.GetKeyPrefix()
	argvs := []interface{}{}
	for _, key := range keys {
		argvs = append(argvs, key)
	}
	argvs = append(argvs, prefix)
	// Serialize the job data as part of the script
	data, err := job.Serialize()
	if err != nil {
		return "", err
	}
	n := len(data)
	parts := make([]string, n*2, n*2)
	for key, value := range data {
		parts = append(parts, key)
		argvs = append(argvs, key)
		parts = append(parts, value)
		argvs = append(argvs, value)
	}
	hmData := ""
	for i := range parts {
		if len(hmData) > 0 {
			hmData += ", "
		}
		hmData += "ARGV["
		// ARGV[1] is the key prefix for the job, so we start at ARGV[2]
		hmData += strconv.Itoa(i + 2)
		hmData += "]"
	}
	// Construct the shared parts of the script
	segments := []string{
		`local jobId = redis.call("INCR", KEYS[4])`,
		fmt.Sprintf(`redis.call("HMSET", ARGV[1] .. jobId, %s)`, hmData),
	}
	// Check if the job is a delayed job
	eta := job.Timestamp + job.Delay
	var _segments []string
	if job.Delay > 0 && eta > time.Now().Unix() {
		// If so, add to the delayed zset
		argvs = append(argvs, eta)
		index := len(parts) + 2
		_segments = []string{
			fmt.Sprintf(`local score = tonumber(ARGV[%d]) * 0x1000 + bit.band(jobId, 0xfff)`, eta),
			`redis.call("ZADD", KEYS[3], score, jobId)`,
			`redis.call("PUBLISH", KEYS[3], (score / 0x1000))`,
		}
	} else {
		// Otherwise, add to the wait list
		push := "LPUSH"
		if lifo {
			push = "RPUSH"
		}
		_segments = []string{
			`redis.call("LPUSH", KEYS[2], jobId)`,
			`redis.call("PUBLISH", KEYS[1], jobId)`,
		}
	}
	for _, segment := range segments {
		segments = append(segments, segment)
	}
	segments = append(segments, `return jobId`)
	// Execute the script
	script := redis.NewScript(len(keys), strings.Join(segments, "\n"))
	conn := bull.pool.Get()
	defer conn.Close()
	result, err := redis.String(script.Do(conn, argvs...))
	if err != nil {
		return "", err
	}
	return result, nil
}

/*
DoGetJob retrieves the job data from the queue
FIXME
*/
func DoGetJob(id string) (*map[string]string, error) {
	return nil, nil
}
