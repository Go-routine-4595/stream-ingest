package main

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"githb.com/Go-routine-4595/stream-ingest/cmd"
)

func main() {
	log.Logger = initializeLogger(0)
	cmd.Execute()
}

// createLogger initializes and returns a new `zerolog.Logger` configured with the given log level.
// It sets the output to `os.Stdout` with RFC3339 time format and includes the process PID in the log context.
func initializeLogger(logLevel int) zerolog.Logger {
	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.Level(logLevel)+zerolog.InfoLevel).
		With().
		Timestamp().
		Int("pid", os.Getpid()).
		Logger()
}
