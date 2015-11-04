package redisdata
import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/amrhassan/agentcontroller2/core"
)

func TestImplementsCoreIncoming(t *testing.T) {
	assert.Implements(t, (*core.Incoming)(nil), new(RedisData))
}

func TestImplementsCoreCommandLogger(t *testing.T) {
	assert.Implements(t, (*core.CommandLogger)(nil), new(RedisData))
}