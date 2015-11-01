package redismb

import (
	"github.com/Jumpscale/agentcontroller2/commands"
	"github.com/Jumpscale/agentcontroller2/messaging"
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"strings"
	"fmt"
)

type redisMessagingBus struct {
	pool *redis.Pool
}

const (
	cmdQueueMain = "cmds.queue"
)

func (messagingBus redisMessagingBus) ReceiveCommand() (*commands.Command, error) {

	db := messagingBus.pool.Get()
	defer db.Close()

	message, err := redis.Strings(db.Do("BLPOP", cmdQueueMain, "0"))
	if err != nil {

		if strings.Contains(err.Error(), "timeout") {
			// Not an error. No command is queued
			return nil, nil
		}

		return nil, redisMBError{underlying: err, errorType: channelErrorType}
	}

	commandText := message[1]
	var command commands.Command

	err = json.Unmarshal([]byte(commandText), &command)
	if err != nil {
		return nil, redisMBError{underlying: err, errorType: commandFormatError}
	}

	return &command, nil
}

func (messagingBus redisMessagingBus) ErrorClassifier() messaging.MessagingBusErrorClassifier {
	return errorClassifier{}
}

func getAgentQueue(gid, nid uint) string {
	return fmt.Sprintf("cmds:%d:%d", gid, nid)
}

func (messagingBus redisMessagingBus) DispatchCommandToAgent(gid, nid uint, command *commands.Command) error {
	db := messagingBus.pool.Get()
	defer db.Close()

	_, err := db.Do("RPUSH", getAgentQueue(gid, nid), command)
	if err != nil {
		return redisMBError{underlying: err, errorType: channelErrorType}
	}

	return nil
}
