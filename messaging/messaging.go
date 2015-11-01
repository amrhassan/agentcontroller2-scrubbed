
package messaging
import "github.com/Jumpscale/agentcontroller2/commands"

// The messaging bus on which AgentController communicates with Agent instances
type MessagingBus interface {

	// Receives and returns a single command
	ReceiveCommand() (*commands.Command, error)

	// An associated error classifier
	ErrorClassifier() MessagingBusErrorClassifier
}
