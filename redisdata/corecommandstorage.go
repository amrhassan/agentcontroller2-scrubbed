package redisdata
import (
	"github.com/amrhassan/agentcontroller2/core"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
	"time"
	"encoding/json"
)

var redisErrorMessage = "Redis error"

type RedisCommandStorage struct {
	pool *redis.Pool
	channels map[core.AgentID](chan core.Command)
	channelLock sync.RWMutex
}

func NewRedisCommandStorage(pool *redis.Pool) core.CommandStorage {
	return &RedisCommandStorage{
		pool: pool,
		channels: make(map[core.AgentID](chan core.Command)),
	}
}

func (store *RedisCommandStorage) QueueReceivedCommand(agentID core.AgentID, command *core.Command) error {

	db := store.pool.Get()
	defer db.Close()

	_, err := db.Do("RPUSH", getAgentQueue(agentID), command)
	if err != nil {
		return fmt.Errorf("%s: %v", redisErrorMessage, err)
	}

	return nil
}


// Communication errors in the command-producing channels are swallowed and handled discretely
func (store *RedisCommandStorage) CommandsForAgent(agentID core.AgentID) (chan <- core.Command) {

	// Beware: Messy mutex synchronization ahead!

	store.channelLock.RLock()
	channel, exists := store.channels[agentID]
	if exists {
		defer store.channelLock.RUnlock()
		return channel
	}

	store.channelLock.RUnlock()
	store.channelLock.Lock()
	defer store.channelLock.Unlock()

	channel = make(chan core.Command)
	go func() {
		conn := store.pool.Get()
		defer conn.Close()

		for {
			redisResponse, err := redis.Strings(conn.Do("BLPOP", getAgentQueue(agentID)))
			if err != nil {
				// Re-try in 1 second
				time.Sleep(1 * time.Second)
				continue
			}

			var command core.Command
			err = json.Unmarshal([]byte(redisResponse[1]), &command)
			if err != nil {
				panic("Malformed command in an Agent queue, should never happen!")
			}

			channel <- command
		}
	}()

	store.channels[agentID] = channel
	return channel
}

func (store *RedisCommandStorage) ReportUndeliveredCommand(agentID core.AgentID, command *core.Command) error {
	conn := store.pool.Get()
	defer conn.Close()

	commandJson, err := json.Marshal(&command)
	if err != nil {
		panic("Failed to marshal a Command!")
	}

	_, err = conn.Do("LPUSH", getAgentQueue(agentID), commandJson)
	return err
}