package redismb

import "fmt"

type errorType string

const (
	channelErrorType errorType = "Redis error"
	commandFormatError errorType = "Command format error"
)

type redisMBError struct {
	underlying error
	errorType  errorType
}

func (err redisMBError) Error() string {
	return fmt.Sprint("%s: %s", string(err.errorType), err.underlying.Error())
}
