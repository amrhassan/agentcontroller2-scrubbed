// Persistent logs
package logging

import "github.com/Jumpscale/agentcontroller2/commands"

type Logger interface {

	// Log the reception  of a certain command
	LogCommand(command *commands.Command) error
}
