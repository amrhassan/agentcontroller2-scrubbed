// Persistent logs
package logging

import "github.com/Jumpscale/agentcontroller2/core"

type Logger interface {

	// Log the reception  of a certain command
	LogCommand(command *core.Command) error
}
