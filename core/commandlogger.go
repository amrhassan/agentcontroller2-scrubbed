package core

type CommandLogger interface {

	// Log the reception  of a certain command
	LogCommand(command *Command) error
}