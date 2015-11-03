package redisdata
import (
	"github.com/garyburd/redigo/redis"
	"fmt"
"github.com/Jumpscale/agentcontroller2/core"
)

type RedisData struct {
	pool *redis.Pool
}


// Constructs and returns a new RedisData instance which implements the following interfaces:
// 	- core.Incoming
//	- core.CommandStorage
//	- core.CommandLogger
func NewRedisData(redisPool *redis.Pool) *RedisData {
	return &RedisData{
		pool: redisPool,
	}
}

const (
	cmdQueueMain              = "cmds.queue"
	cmdQueueCmdQueued         = "cmd.%s.queued"
	cmdQueueAgentResponse     = "cmd.%s.%d.%d"
	hashCmdResults            = "jobresult:%s"
)

func getAgentQueue(agentID core.AgentID) string {
	return fmt.Sprintf("cmds:%d:%d", agentID.GID, agentID.NID)
}