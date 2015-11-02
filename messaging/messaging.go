
package messaging
import "github.com/Jumpscale/agentcontroller2/commands"

type AgentID struct {
	GID uint
	NID uint
}

// The messaging bus on which AgentController communicates with Agent instances
type MessagingBus interface {

	// Receives and returns a single command without blocking, returning nil if there weren't any
	ReceiveCommand() (*commands.Command, error)

	// Queues a received command to be dispatched to an Agent somewhere
	QueueCommandToAgent(agentID AgentID, command *commands.Command) error

	// Responds to a received command and acknowledges that it's been queued to the specified agent
	RespondToCommandAsJustQueued(agentID AgentID, command *commands.Command) error

	// Publishes an update to a command's result
	SetCommandResult(commandResult *commands.Result) error

	// Signals a command as queued
	// TODO: Figure out why this is necessary and find a cleaner way to do this in another command
	SignalCommandAsQueued(commandID string) error

	// An associated error classifier
	ErrorClassifier() MessagingBusErrorClassifier
}
