package bullion

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// BullionPrefixDefault is the default key prefix
const BullionPrefixDefault = "bull"

// BullionKeyJobs is the key name of the jobs channel
const BullionKeyJobs = "jobs"

// BullionKeyJobsCount is the key for the jobs count
const BullionKeyJobsCount = "id"

const (
	// BullionKeyWait is the key name of the wait list
	BullionKeyWait = "wait"
	// BullionKeyActive is the key name of the active list
	BullionKeyActive = "active"
	// BullionKeyDelayed is the key name of the delayed zset
	BullionKeyDelayed = "delayed"
	// BullionKeyCompleted is the key name of the completed set
	BullionKeyCompleted = "completed"
	// BullionKeyFailed is the key name of the failed set
	BullionKeyFailed = "failed"
)

// Bullion is the main bullion program
type Bullion struct {
	Name string // queue name

	prefix  string // queue prefix
	pool    *redis.Pool
	bclient redis.Conn
	eclient redis.PubSubConn
}

// ConnectOptions is the paramters for connecting to redis
type ConnectOptions struct {
	Address string // redis address
	Auth    string // redis password
	DB      string // redis db number
	Name    string // queue name
}

// New creates a bullion instance
func New(options *ConnectOptions) *Bullion {
	// Create a redis pool
	pool := &redis.Pool{
		MaxIdle:     3,                 // max idle connections
		IdleTimeout: 240 * time.Second, // idle timeout
		Dial: func() (redis.Conn, error) {
			// Construct connection
			conn, err := redis.Dial("tcp", options.Address)
			if err != nil {
				return nil, err
			}
			// Authenticate if necessary
			if options.Auth != "" {
				if _, err := conn.Do("AUTH", options.Auth); err != nil {
					conn.Close()
					return nil, err
				}
			}
			// Select db if necessary
			if options.DB != "" {
				if _, err := conn.Do("SELECT", options.DB); err != nil {
					conn.Close()
					return nil, err
				}
			}
			return conn, nil
		},
	}
	// Create connections
	bclient := pool.Get()
	eclient := redis.PubSubConn{
		Conn: pool.Get(),
	}
	// Construct bullion
	bull := &Bullion{
		prefix:  BullionPrefixDefault,
		pool:    pool,
		bclient: bclient,
		eclient: eclient,
	}
	return bull
}

// Close terminates all connections to the queue
func (bull *Bullion) Close() error {
	err := bull.bclient.Close()
	if err != nil {
		return err
	}
	err = bull.eclient.Close()
	if err != nil {
		return err
	}
	return nil
}

// GetKeyPrefix returns the key prefix for redis
func (bull *Bullion) GetKeyPrefix() string {
	return bull.prefix + ":" + bull.Name + ":"
}

// GetKey returns the full key for a key segment
func (bull *Bullion) GetKey(segment string) string {
	return bull.GetKeyPrefix() + segment
}
