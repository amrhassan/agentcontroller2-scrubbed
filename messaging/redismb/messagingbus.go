package redismb

import (
	"github.com/Jumpscale/agentcontroller2/commands"
	"github.com/Jumpscale/agentcontroller2/messaging"
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"strings"
	"fmt"
	"time"
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

func getAgentQueue(agentID messaging.AgentID) string {
	return fmt.Sprintf("cmds:%d:%d", agentID.GID, agentID.NID)
}

func (messagingBus redisMessagingBus) DispatchCommandToAgent(agentID messaging.AgentID,
	command *commands.Command) error {

	db := messagingBus.pool.Get()
	defer db.Close()

	_, err := db.Do("RPUSH", getAgentQueue(agentID), command)
	if err != nil {
		return redisMBError{underlying: err, errorType: channelErrorType}
	}

	return nil
}

func (messagingBus redisMessagingBus) RespondToCommandAsJustQueued(agentID messaging.AgentID,
	command *commands.Command) error {

	db := messagingBus.pool.Get()
	defer db.Close()

	resultPlaceholder := commands.Result{
		ID:        command.ID,
		Gid:       int(agentID.GID),
		Nid:       int(agentID.NID),
		State:     commands.STATE_QUEUED,
		StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
	}

	data, err := json.Marshal(&resultPlaceholder)

	if err != nil {
		panic(fmt.Sprintf("Failed to marshal JSON for some reason!! %s", err))
	}

	_, err =
		db.Do("HSET", fmt.Sprintf("jobresult:%s", command.ID), fmt.Sprintf("%d:%d", agentID.GID, agentID.NID), data)

	if err != nil {
		return redisMBError{underlying: err, errorType: channelErrorType}
	}

	return nil
}
