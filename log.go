package migrate // import "github.com/shanna/migrate"

import (
	golog "log"
)

// LoggerFunc is an INFO logging function. Everything else is an error.
type LoggerFunc func(format string, args ...interface{})

var log = LoggerFunc(LoggerLog)

// SetLogger function.
func SetLogger(f LoggerFunc) {
	log = f
}

// LoggerNil will suppress logs.
func LoggerNil(format string, args ...interface{}) {}

// LoggerLog logs to log.Printf
func LoggerLog(format string, args ...interface{}) {
	golog.Printf(format, args...)
}
