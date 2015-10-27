package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/pborman/uuid"
	"github.com/robfig/cron"
	"log"
)

const (
	hashScheduleKey = "controller.schedule"
)

type Scheduler struct {
	cron *cron.Cron
	pool *redis.Pool
}

type SchedulerJob struct {
	ID   string                 `json:"id"`
	Cron string                 `json:"cron"`
	Cmd  map[string]interface{} `json:"cmd"`
}

func (job *SchedulerJob) Run() {
	db := pool.Get()
	defer db.Close()

	job.Cmd["id"] = uuid.New()

	dump, _ := json.Marshal(job.Cmd)

	_, err := db.Do("RPUSH", cmdQueueMain, string(dump))
	if err != nil {
		log.Println("Failed to run scheduled command", job.ID)
	}
}

func NewScheduler(pool *redis.Pool) *Scheduler {
	sched := &Scheduler{
		cron: cron.New(),
		pool: pool,
	}

	return sched
}

//create a schdule with the cmd ID (overrides old ones) and
func (sched *Scheduler) Add(cmd *CommandMessage) (interface{}, error) {
	defer sched.restart()

	db := sched.pool.Get()
	defer db.Close()

	job := &SchedulerJob{}

	err := json.Unmarshal([]byte(cmd.Data), job)

	if err != nil {
		log.Println("Failed to load command spec", cmd.Data, err)
		return nil, err
	}

	_, err = cron.Parse(job.Cron)
	if err != nil {
		return nil, err
	}

	job.ID = cmd.ID
	//we can safely push the command to the hashset now.
	db.Do("HSET", hashScheduleKey, cmd.ID, cmd.Data)

	return true, nil
}

func (sched *Scheduler) List(cmd *CommandMessage) (interface{}, error) {
	db := sched.pool.Get()
	defer db.Close()

	return redis.StringMap(db.Do("HGETALL", hashScheduleKey))
}

func (sched *Scheduler) Remove(cmd *CommandMessage) (interface{}, error) {
	db := sched.pool.Get()
	defer db.Close()

	value, err := redis.Int(db.Do("HDEL", hashScheduleKey, cmd.ID))

	if value > 0 {
		//actuall job was deleted. need to restart the scheduler
		sched.restart()
	}

	return value, err
}

func (sched *Scheduler) restart() {
	sched.cron.Stop()
	sched.cron = cron.New()
	sched.Start()
}

func (sched *Scheduler) Start() {
	db := sched.pool.Get()
	defer db.Close()

	var cursor int
	for {
		slice, err := redis.Values(db.Do("HSCAN", hashScheduleKey, cursor))
		if err != nil {
			log.Println("Failed to load schedule from redis", err)
			break
		}

		var fields interface{}
		if _, err := redis.Scan(slice, &cursor, &fields); err == nil {
			set, _ := redis.StringMap(fields, nil)

			for key, cmd := range set {
				job := &SchedulerJob{}

				err := json.Unmarshal([]byte(cmd), job)

				if err != nil {
					log.Println("Failed to load scheduled job", key, err)
					continue
				}

				job.ID = key
				sched.cron.AddJob(job.Cron, job)
			}
		} else {
			log.Println(err)
			break
		}

		if cursor == 0 {
			break
		}
	}

	sched.cron.Start()
}
