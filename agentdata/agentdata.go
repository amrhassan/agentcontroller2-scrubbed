package agentdata
import (
	"github.com/amrhassan/agentcontroller2/core"
	"sync"
)

type agentData struct {
	roles	map[core.AgentID]([]core.AgentRole)
	lock	sync.RWMutex
}

func (data *agentData) SetRoles(id core.AgentID, roles []core.AgentRole) {
	data.lock.Lock()
	defer data.lock.Unlock()

	nRoles := make([]core.AgentRole, len(roles))
	copy(nRoles, roles)
	data.roles[id] = nRoles
}

func (data *agentData) GetRoles(id core.AgentID) []core.AgentRole {
	data.lock.RLock()
	defer data.lock.RUnlock()

	roles, rolesExist := data.roles[id]
	if !rolesExist {
		return nil
	}

	nRoles := make([]core.AgentRole, len(roles))
	copy(nRoles, roles)

	return nRoles
}

func (data *agentData) DropAgent(id core.AgentID) {
	data.lock.Lock()
	defer data.lock.Unlock()

	_, exists := data.roles[id]
	if exists {
		delete(data.roles, id)
	}
}

func (data *agentData) ConnectedAgents() []core.AgentID {
	data.lock.RLock()
	defer data.lock.RUnlock()

	var agents []core.AgentID
	for agentID := range data.roles {
		agents = append(agents, agentID)
	}

	return agents
}

func (data *agentData) HasRole(id core.AgentID, role core.AgentRole) bool {
	data.lock.RLock()
	defer data.lock.RUnlock()

	roles, exist := data.roles[id]

	if !exist {
		return false
	}

	for _, attachedRole := range roles {
		if attachedRole == role {
			return true
		}
	}

	return false
}

func (data *agentData) IsConnected(id core.AgentID) bool {
	data.lock.RLock()
	defer data.lock.RUnlock()

	for agentID, _ := range data.roles {
		if agentID == id {
			return true
		}
	}

	return false
}

// Constructs a new thread-safe in-memory implementation of core.AgentInformationStorage
func NewAgentData() core.AgentInformationStorage {
	return &agentData{
		roles: make(map[core.AgentID]([]core.AgentRole)),
	}
}