// Package cmd
// -----------------------------------------------------------------------------
// File: verify.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            It provides functionalities to interact with the user and
//	            verify the input streams definition file syntax
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
package cmd

import (
	"errors"
	"fmi/stream-ingest/model"
	"fmt"
	"io"

	"fmi/stream-ingest/internal"
	"fmi/stream-ingest/repository/dataprocessor"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// verifyCmd handles the "verify" command
var verifyCmd = &cobra.Command{
	Use:   "verify [file]",
	Short: "Verify the syntax of the given file for ingestion compatibility",
	Args:  cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		fmt.Printf("Verifying syntax of file: %s\n", file)

		// Call your logic to verify the syntax of the file here
		executeVerify(file)
	},
}

func init() {
	rootCmd.AddCommand(verifyCmd)
}

func executeVerify(file string) {
	var (
		err     error
		logRecs []internal.LogRecord
	)

	issue := false

	regEle, err := model.NewRegistry(file)
	if err != nil {
		fmt.Printf("unregonize csv header: %v \n", err)
		return
	}

	reader, err := dataprocessor.NewCSVReader(file, "", regEle.GetHeaders())
	if err != nil {
		if errors.Is(err, dataprocessor.UnknownTagErr) {
			fmt.Println(err)
		} else {
			fmt.Println(err)
			return
		}
	}

	sensorId := make(map[string]int)

	defer reader.Close()

	lineNumber, err := reader.CountLines()
	bar, bucket, remainder := progressBar(lineNumber, "Processing file "+file)
	defer bar.Finish()

	// We skip the first line (header)
	err = reader.SkipLine()
	if err != nil {
		logRecs = append(logRecs, internal.LogRecord{Err: err, Msg: "Failed to skip header line"})
		return
	}

	for i := 2; ; i++ {
		if i%bucket == 0 {
			_ = bar.Add(bucket)
		}
		streamRes := regEle.NewElement("")
		err = reader.ReadNext(streamRes)
		if err != nil {
			if err == io.EOF {
				break
			}
			logRecs = append(logRecs, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Failed to read next stream on line: %d", i)})
			issue = true
			continue
		}
		// check is a row had the same sensorId we already processed in the file
		// SensorID is the primaryKey
		if _, ok := sensorId[streamRes.GetID()]; ok {
			logRecs = append(logRecs, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[streamRes.GetID()])})
		} else {
			sensorId[streamRes.GetID()] = i
		}
	}
	_ = bar.Add(remainder)
	fmt.Println()
	if !issue {
		fmt.Println("")
		log.Logger.Info().Msg("Syntax is valid")
	}
	if len(logRecs) > 0 {
		fmt.Println("")
		internal.PrintLogRecord(logRecs)
	}
}
