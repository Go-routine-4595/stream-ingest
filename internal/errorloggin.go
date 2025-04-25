// Package internal
// -----------------------------------------------------------------------------
// File: verify.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            Log and Error log management
//
// Author: <Christophe Buffard>
// Created: <01/15/2025>
// -----------------------------------------------------------------------------
// Notes:
//   - This file is part of the FCTS/stream ingestion project.
//   - Updated/reliable documentation and usage examples can be found at:
//     <Link to project README or documentation>
//
// -----------------------------------------------------------------------------
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

func PrintLogErrRecord(logRcords []LogRecord) {
	if len(logRcords) > 0 {
		for _, logR := range logRcords {
			if logR.Err != nil {
				log.Logger.Err(logR.Err).Msgf("%s", logR.Msg)
			}
		}
	}
}

func AddError(logRecords *[]LogRecord, err []error, msg string) {
	for _, e := range err {
		*logRecords = append(*logRecords, LogRecord{Err: e, Msg: msg})
	}
}
