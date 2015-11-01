package messaging

type MessagingBusErrorClassifier interface {

	// Checks if the error was produced in the underlying message bus
	IsChannelError(err error) bool

	// Checks if the error is about a badly formatted command
	IsCommandFormatError(err error) bool
}
