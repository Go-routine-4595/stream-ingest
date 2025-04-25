// Package cmd
// -----------------------------------------------------------------------------
// File: check.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            It provides functionalities to interact with the user and
//	            verify the input streams definition file syntax and check if
//				the streams already exist in the CosmosDB
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
	"github.com/rs/zerolog/log"
	"io"

	"fmi/stream-ingest/internal"
	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"

	"github.com/spf13/cobra"
)

// checkCmd handles the "check" command
var checkCmd = &cobra.Command{
	Use:   "check [file]",
	Short: "Check if the data in the given file already exists in the database",
	Args:  cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		debug, _ := cmd.Flags().GetBool("debug")
		verb, _ := cmd.Flags().GetBool("verbose")
		prod, _ := cmd.Flags().GetBool("prod")
		var instance string
		if prod {
			instance = "Prod"
		} else {
			instance = "Dev"
		}
		fmt.Printf("Checking if data in file %s exists in the database %s \n", file, instance)
		// Call your logic to check the file contents against the database here
		executeCheck(file, debug, instance, verb)
	},
}

func init() {
	checkCmd.Flags().BoolP("debug", "d", false, "debug mode, will save all UUID (id) in the log file.")
	checkCmd.Flags().BoolP("prod", "p", false, "Production CosmosDB used.")
	checkCmd.Flags().BoolP("verbose", "v", false, "verbose mode, will print all log messages.")

	rootCmd.AddCommand(checkCmd)
}

func executeCheck(file string, debug bool, instance string, verb bool) {
	var (
		err                error
		logRecs            []internal.LogRecord
		recordsToBeUpdated int
		recordsToBeCreated int
	)

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

	defer reader.Close()

	repo := cosmos.NewRepository(instance)
	sensorId := make(map[string]int)

	lineNumber, err := reader.CountLines()
	bar, bucket, remainder := progressBar(lineNumber, "Processing file "+file)
	defer bar.Finish()

	//fmt.Printf("bucket size: %d \n", bucket)

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
			internal.PrintLogRecord(logRecs)
			return
		}
		// check is a row had the same sensorId we already processed in the file
		// SensorID is the primaryKey
		if _, ok := sensorId[streamRes.GetID()]; ok {
			logRecs = append(logRecs, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[streamRes.GetID()])})
			//internal.PrintLogRecord(logRecs)
			continue
		} else {
			sensorId[streamRes.GetID()] = i
		}
		// SensorID is the primaryKey
		//storedSteams, err := repo.GetStreamByStreamIdAndSiteCode(streamRes.GetID(), streamRes.GetSiteCode())
		storeStreams, err := regEle.GetElementFromRepo(repo, streamRes.GetID(), streamRes.GetSiteCode())
		if err != nil {
			logRecs = append(logRecs, internal.LogRecord{Err: err, Msg: "Failed to get stream"})
			internal.PrintLogRecord(logRecs)
			return
		}
		if len(storeStreams) == 0 {
			logRecs = append(logRecs, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("stream \"%s\" at line: %d  in file: %s does not exist in the Registry", streamRes.GetID(), i, file)})
			recordsToBeCreated++
			continue
		}
		if len(storeStreams) == 1 {
			err = storeStreams[0].Validate()
			if err != nil {
				logRecs = append(logRecs, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Registry stream:\"%s\" in ComsosDB has duplicated tag", storeStreams[0].GetID())})
				internal.PrintLogRecord(logRecs)
				return
			}
			//stream.CompareStreams(storedSteams[0], streamRes)
			if !storeStreams[0].CompareTo(streamRes) {
				logRecs = append(logRecs, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("Registry stream:\"%s\" need to be updated by file: %s row line: %d ", storeStreams[0].GetID(), file, i)})
				recordsToBeUpdated++
			}

		}
		if len(storeStreams) > 1 {
			logRecs = append(logRecs, internal.LogRecord{Err: err, Msg: fmt.Sprintf("stream \"%s\" at line: %d  in file: %s appears more than once in the Registry", streamRes.GetID(), i, file)})
			if debug {
				for _, fetchedStream := range storeStreams {
					logRecs = append(logRecs, internal.LogRecord{Err: errors.New("multiple stream defined"), Msg: fmt.Sprintf("sensorId: \"%s\" stream.ID: \"%s\"", fetchedStream.GetID(), fetchedStream.GetInternalID())})
				}
				internal.PrintLogRecord(logRecs)
				return
			}
		}
	}
	_ = bar.Add(remainder)
	fmt.Println("")
	if verb {
		internal.PrintLogErrRecord(logRecs)
	}
	log.Info().Msgf(
		"There are %6d streams/constants that need to be updated",
		recordsToBeUpdated)
	log.Info().Msgf(
		"There are %6d streams/constants that need to be created",
		recordsToBeCreated)
}
