package redisdata
import "github.com/Jumpscale/agentcontroller2/core"


func (redisData *RedisData) LogCommand(command *core.Command) error {
	db := redisData.pool.Get()
	defer db.Close()

	_, err := db.Do("LPUSH", "joblog", command)
	return err
}
