package core

type AgentID struct {
	GID uint
	NID uint
}


// Information about all things Agents
type AgentInformationStorage interface {

	// Sets the roles associated with an Agent
	SetRoles(id AgentID, roles [][]string)

	// Gets the roles associated with an Agent
	GetRoles(id AgentID) [][]string

	// Drops all the known information about an Agent
	DropAgent(id AgentID)

	// Queries for all the available Agents
	ConnectedAgents() []AgentID
}