package core

// Persisted storage for incoming commands
type CommandStorage interface {

	// Queues a received command
	QueueReceivedCommand(agentID AgentID, command *Command) error

	// Returns a channel of commands for the specified agents
	// Always returns the same channel for the same AgentID
	CommandsForAgent(agentID AgentID) (chan <- Command)

	// Report that this command was dequeued for delivery to that agent but delivery to said Agent
	// failed for one reason or another
	ReportUndeliveredCommand(agentID AgentID, command *Command)
}