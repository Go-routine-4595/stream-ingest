package internal

import "github.com/rs/zerolog/log"

type LogRecord struct {
	Err error
	Msg string
}

func PrintLogRecord(logRcords []LogRecord) {
	if len(logRcords) > 0 {
		for _, logR := range logRcords {
			log.Logger.Err(logR.Err).Msgf("%s", logR.Msg)
		}
	}
}

func AddError(logRecords *[]LogRecord, err []error, msg string) {
	for _, e := range err {
		*logRecords = append(*logRecords, LogRecord{Err: e, Msg: msg})
	}
}
