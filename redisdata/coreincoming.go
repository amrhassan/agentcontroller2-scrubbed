package redisdata
import (
	"github.com/Jumpscale/agentcontroller2/core"
	"github.com/garyburd/redigo/redis"
	"strings"
	"encoding/json"
	"fmt"
)

var commandFormatErrorMessage = "Command format error"

func (redisData *RedisData) ReceiveCommand() (*core.Command, error) {

	db := redisData.pool.Get()
	defer db.Close()

	message, err := redis.Strings(db.Do("BLPOP", cmdQueueMain, "0"))
	if err != nil {

		if strings.Contains(err.Error(), "timeout") {
			// Not an error. No command is queued
			return nil, nil
		}

		return nil, fmt.Errorf("%s: %v", commandFormatErrorMessage, err)
	}

	commandText := message[1]
	var command core.Command

	err = json.Unmarshal([]byte(commandText), &command)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", commandFormatErrorMessage, err)
	}

	return &command, nil
}

func (store *RedisData) IsCommandFormatError(err error) bool {
	return strings.HasPrefix(err.Error(), commandFormatErrorMessage)
}
