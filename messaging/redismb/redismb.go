package redismb

import (
	"github.com/Jumpscale/agentcontroller2/messaging"
	"github.com/garyburd/redigo/redis"
)

func NewRedisMessagingBus(redisPool *redis.Pool) messaging.MessagingBus {
	return &redisMessagingBus{
		pool: redisPool,
	}
}
