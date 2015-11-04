package core

type AgentID struct {
	GID uint
	NID uint
}

type AgentRole string

// Information about all things Agents
type AgentInformationStorage interface {

	// Sets the roles associated with an Agent
	SetRoles(id AgentID, roles []AgentRole)

	// Gets the roles associated with an Agent
	GetRoles(id AgentID) []AgentRole

	// Drops all the known information about an Agent
	DropAgent(id AgentID)

	// Checks if the specified agent has the specified role
	HasRole(id AgentID, role AgentRole) bool

	// Queries for all the available Agents
	ConnectedAgents() []AgentID

	// Queries for all the available agents that specify the given criteria:
	//	- If gid is not nil, only returns IDs of Agents with that GID
	//	- if roles is not nil, only returns IDs of Agents that have all of these roles
	FilteredConnectedAgents(gid *uint, roles []AgentRole) []AgentID

	IsConnected(id AgentID) bool
}