package theproducers
import (
	"github.com/amrhassan/agentcontroller2/core"
	"sync"
	"log"
	"time"
	"encoding/json"
)

const agentInteractiveAfterOver = 30 * time.Second

/*
PollData Gets a chain for the caller to wait on, we return a chan chan string instead
of chan string directly to make sure of the following:
1- The redis pop loop will not try to pop jobs out of the queue until there is a caller waiting
   for new commands
2- Prevent multiple clients polling on a single gid:nid at the same time.
*/
type PollData struct {
	Roles   []string
	MsgChan chan string
}

type Producer chan PollData

// A manager of Producer instances
type Producers struct {
	running	map[core.AgentID]Producer
	lock sync.RWMutex
	agentData core.AgentInformationStorage
	commandStorage core.CommandStorage
}

func NewProducers(agentData core.AgentInformationStorage, commandStorage core.CommandStorage) *Producers {
	return &Producers{
		running: make(map[core.AgentID]Producer),
		agentData: agentData,
		commandStorage: commandStorage,
	}
}

func (producers *Producers) newProducer(agentID core.AgentID) Producer {
	producer := make(Producer)
	go producerLogic(producers.commandStorage, producers.agentData, producer, agentID)
	return producer
}


func (producers *Producers) Get(agentID core.AgentID) Producer {
	producers.lock.RLock()
	producer, exists := producers.running[agentID]

	if exists {
		producers.lock.RUnlock()
		return producer
	}

	producers.lock.RUnlock()
	producers.lock.Lock()
	defer producers.lock.Unlock()

	producer = producers.newProducer(agentID)
	producers.running[agentID] = producer

	return producer
}

func producerLogic(commandStorage core.CommandStorage, agentData core.AgentInformationStorage,
	producer Producer, agentID core.AgentID) {

	defer func() {
		//no agent tried to connect
		close(producer)
		agentData.DropAgent(agentID)
	}()

	for {
		var data PollData

		select {
		case data = <-producer:

		case <-time.After(agentInteractiveAfterOver):
		//no active agent for 10 min
			log.Println(agentID, "is inactive for over ", agentInteractiveAfterOver, ", cleaning up.")
			continue
		}

		msgChan := data.MsgChan
		defer close(msgChan)

		roles := data.Roles

		var agentRoles []core.AgentRole
		for role := range roles {
			agentRoles = append(agentRoles, core.AgentRole(role))
		}
		agentData.SetRoles(agentID, agentRoles)


		command := <- commandStorage.CommandsForAgent(agentID)
		jsonCommand, err := json.Marshal(command)
		if err != nil {
			panic(err)
		}

		select {
		case msgChan <- string(jsonCommand):
		//caller consumed this job, it's safe to set it's state to RUNNING now.

			resultPlacehoder := core.CommandResult{
				ID:        command.ID,
				Gid:       command.Gid,
				Nid:       command.Nid,
				State:     "RUNNING",
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			commandStorage.SetCommandResult(&resultPlacehoder)

		default:
			//caller didn't want to receive this command. have to repush it
			//directly on the agent queue. to avoid doing the redispatching.
			commandStorage.ReportUndeliveredCommand(agentID, &command)
		}
	}
}