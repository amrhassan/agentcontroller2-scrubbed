
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

	// Dispatches a received command to an Agent somewhere
	DispatchCommandToAgent(agentID AgentID, command *commands.Command) error

	// Responds to a received command and acknowledges that it's been queued to the specified agent
	RespondToCommandAsJustQueued(agentID AgentID, command *commands.Command) error

	// Publishes an update to a command's result
	SetCommandResult(commandResult *commands.Result) error

	// An associated error classifier
	ErrorClassifier() MessagingBusErrorClassifier
}
