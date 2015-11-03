package redisdata
import (
	"github.com/Jumpscale/agentcontroller2/core"
	"fmt"
)

var redisErrorMessage = "Redis error"

func (store *RedisData) QueueReceivedCommand(agentID core.AgentID, command *core.Command) error {

	db := store.pool.Get()
	defer db.Close()

	_, err := db.Do("RPUSH", getAgentQueue(agentID), command)
	if err != nil {
		return fmt.Errorf("%s: %v", redisErrorMessage, err)
	}

	return nil
}


func (store *RedisData) CommandsForAgent(agentID core.AgentID) (chan <- core.Command) {
	// TODO
	panic("TODO")
}

func (store *RedisData) ReportUndeliveredCommand(agentID core.AgentID, command *core.Command) {
	// TODO
	panic("TODO")
}