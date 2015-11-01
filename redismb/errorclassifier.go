package redismb

import "strings"

type errorClassifier struct{}

var theErrorClassifier errorClassifier = errorClassifier{}

func (classifier errorClassifier) IsChannelError(err error) bool {
	return strings.HasPrefix(err.Error(), string(channelErrorType))
}

func (classifier errorClassifier) IsCommandFormatError(err error) bool {
	return strings.HasPrefix(err.Error(), string(commandFormatError))
}