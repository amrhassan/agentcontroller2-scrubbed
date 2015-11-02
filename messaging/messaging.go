
package messaging
import "github.com/Jumpscale/agentcontroller2/core"


// The messaging bus on which AgentController communicates with Agent instances
type MessagingBus interface {

	// Receives and returns a single command without blocking, returning nil if there weren't any
	ReceiveCommand() (*core.Command, error)

	// Queues a received command to be dispatched to an Agent somewhere
	QueueCommandToAgent(agentID core.AgentID, command *core.Command) error

	// Responds to a received command and acknowledges that it's been queued to the specified agent
	RespondToCommandAsJustQueued(agentID core.AgentID, command *core.Command) error

	// Publishes an update to a command's result
	SetCommandResult(commandResult *core.CommandResult) error

	// Signals a command as queued
	// TODO: Figure out why this is necessary and find a cleaner way to do this in another command
	SignalCommandAsQueued(commandID string) error

	// An associated error classifier
	ErrorClassifier() MessagingBusErrorClassifier
}
