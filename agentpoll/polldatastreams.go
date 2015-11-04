// Handling of tricky Agent polling logic. Should eventually be made a lot simpler.
package agentpoll
import (
	"github.com/amrhassan/agentcontroller2/core"
	"sync"
	"log"
	"time"
)

// An agent is considered offline if it doesn't send any data in this amount of time
const offlineAgentInactivityTimeout = 30 * time.Second

/*
PollData Gets a chain for the caller to wait on, we return a chan chan string instead
of chan string directly to make sure of the following:
1- The redis pop loop will not try to pop jobs out of the queue until there is a caller waiting
   for new commands
2- Prevent multiple clients polling on a single gid:nid at the same time.
*/
type PollData struct {
	Roles   []core.AgentRole
	CommandChannel chan core.Command
}

type PollDataStream chan PollData

// A manager of Handler instances
type PollDataStreamManager struct {
	running	map[core.AgentID]PollDataStream
	lock sync.RWMutex
	agentData core.AgentInformationStorage
	commandStorage core.CommandStorage
}

func NewManager(agentData core.AgentInformationStorage, commandStorage core.CommandStorage) *PollDataStreamManager {
	return &PollDataStreamManager{
		running: make(map[core.AgentID]PollDataStream),
		agentData: agentData,
		commandStorage: commandStorage,
	}
}

func (manager *PollDataStreamManager) newProducer(agentID core.AgentID) PollDataStream {
	producer := make(PollDataStream)
	go pollDataStreamLogic(manager.commandStorage, manager.agentData, producer, agentID)
	return producer
}


func (manager *PollDataStreamManager) Get(agentID core.AgentID) PollDataStream {
	manager.lock.RLock()
	producer, exists := manager.running[agentID]

	if exists {
		defer manager.lock.RUnlock()
		return producer
	}

	manager.lock.RUnlock()
	manager.lock.Lock()
	defer manager.lock.Unlock()

	producer = manager.newProducer(agentID)
	manager.running[agentID] = producer

	return producer
}

func pollDataStreamLogic(
	commandStorage core.CommandStorage,
	agentData core.AgentInformationStorage,
	stream PollDataStream,
	agentID core.AgentID) {

	defer close(stream)
	defer agentData.DropAgent(agentID)

	for {

		// Data to be received from the Agent
		var data PollData

		select {

		case data = <-stream:

		case <-time.After(offlineAgentInactivityTimeout):
			log.Println(agentID, "is inactive for over ", offlineAgentInactivityTimeout, ", cleaning up.")
			return
		}

		defer close(data.CommandChannel)

		agentData.SetRoles(agentID, data.Roles)

		command := <- commandStorage.CommandsForAgent(agentID)

		select {
		case data.CommandChannel <- command:

			// Agent consumed this job, it's safe to set it's state to RUNNING now.
			commandResult := core.CommandResult{
				ID:        command.ID,
				Gid:       command.Gid,
				Nid:       command.Nid,
				State:     core.COMMAND_STATE_RUNNING,
				StartTime: int64(time.Duration(time.Now().UnixNano()) / time.Millisecond),
			}

			commandStorage.SetCommandResult(&commandResult)

		default:
			// Agent did not receive this command.
			commandStorage.ReportUndeliveredCommand(agentID, &command)
		}
	}
}