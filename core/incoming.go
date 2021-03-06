package core

// AgentController's command input interface to its clients
type Incoming interface {

	// Receives and returns a single command without blocking, returning nil if there weren't any
	ReceiveCommand() (*Command, error)

	// Checks if the error is about a badly formatted incoming command
	IsCommandFormatError(err error) bool
}