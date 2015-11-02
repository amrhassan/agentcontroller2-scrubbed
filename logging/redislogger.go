package logging

import (
	"github.com/Jumpscale/agentcontroller2/core"
	"github.com/garyburd/redigo/redis"
)

type redisLogger struct {
	pool *redis.Pool
}

func (logger *redisLogger) LogCommand(command *core.Command) error {
	db := logger.pool.Get()
	defer db.Close()

	_, err := db.Do("LPUSH", "joblog", command)
	return err
}

func NewRedisLogger(redisPool *redis.Pool) Logger {
	return &redisLogger{pool: redisPool}
}
