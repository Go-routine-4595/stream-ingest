// Package cmd
// -----------------------------------------------------------------------------
// File: delete.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            Delete all streams by streamId or id.
//				Note: every streams attribute will be updated as follows:
//				- tag will be updated form import file
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
	"encoding/json"
	"errors"
	"fmi/stream-ingest/internal"
	"fmi/stream-ingest/model"
	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// ingestCmd handles the "ingest" command
var deleteCmd = &cobra.Command{
	Use:   "delete [file]",
	Short: "delete data from the given file into the database by streamId or id",
	Long: `delete data from the given file into the database.
input file format:
- SensorId | id`,
	Example: `delete -u 123456789 ./data/streams.csv`,
	Args:    cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		debug, _ := cmd.Flags().GetBool("debug")
		prod, _ := cmd.Flags().GetBool("prod")
		user, _ := cmd.Flags().GetString("user")
		verb, _ := cmd.Flags().GetBool("verbose")

		var instance string
		if prod {
			instance = "Prod"
		} else {
			instance = "Dev"
		}

		question := fmt.Sprintf("Your are about to delete data from file into %s DB, are you sure? (y/N) : ", instance)
		if !isUserOk(question, "") {
			return
		}

		fmt.Printf("Deleting data from file: %s\n", file)

		// Call your logic to ingest the data here
		executeDelete(file, debug, instance, user, verb)
	},
}

func init() {
	// Add the "delete" command and define its flag
	deleteCmd.Flags().BoolP("debug", "d", false, "debug mode, will save all UUID (id) of updated streams and created streams in 2 different log files.")
	deleteCmd.Flags().BoolP("prod", "p", false, "Production CosmosDB used.")
	deleteCmd.Flags().StringP("user", "u", "", "employee id")
	deleteCmd.Flags().BoolP("verbose", "v", false, "verbose mode, will print all log messages.")

	rootCmd.AddCommand(deleteCmd)
}

func executeDelete(file string, debug bool, instance string, user string, verb bool) {
	var (
		err                        error
		fetchedStreams             []model.RegistryInterface
		LogRecords                 []internal.LogRecord
		debugHandlerNewStreams     *internal.DebugData
		debugHandlerUpdatedStreams *internal.DebugData
		streamBackupSaver          *dataprocessor.FileSaver
	)

	regEle, err := model.NewRegistry(file)
	if err != nil {
		fmt.Printf("unregonize csv header: %v \n", err)
		return
	}

	// we create a reader for the input CSV file
	reader, err := dataprocessor.NewCSVReader(file, user, regEle.GetHeaders())
	if err != nil {
		if errors.Is(err, dataprocessor.UnknownTagErr) {
			log.Logger.Err(err).Msg("unknown tag in the CSV file")
			if !isUserOk("Do you want to continue anyway? (y/N) : ", "new tag will be created") {
				return
			}
		} else {
			log.Logger.Err(err).Msg("failed to create reader")
			return
		}
	}

	// we create a writer to persist the issue we have encounter so we know what has not been created
	resFile := getFileNameCsv("import-result")
	persisteUnprocessedStreams, err := dataprocessor.NewCSVPersist(resFile, reader.GetHeaders(), 4096)
	if err != nil {
		log.Logger.Err(err)
		return
	}

	defer reader.Close()
	defer persisteUnprocessedStreams.Close()

	// for a debug mode we store all UUID of the stream we are creating, in case we need to clean that mess up easily :)
	if debug {
		debugHandlerNewStreams, err = internal.NewDebugData(getFileNameJson("stream-id-new"), 4096)
		if err != nil {
			log.Logger.Err(err).Msg("failed to create debug data")
			return
		}
		debugHandlerUpdatedStreams, err = internal.NewDebugData(getFileNameJson("stream-id-updated"), 4096)
		if err != nil {
			log.Logger.Err(err).Msg("failed to create debug data")
			return
		}
		// we add the stream id

		streamBackupSaver, err = dataprocessor.NewFileSaver(getFileNameJson("streams-backup"), 4096)
		if err != nil {
			log.Logger.Err(err).Msg("failed to create debug data")
			return
		}

		defer debugHandlerNewStreams.Close()
		defer debugHandlerUpdatedStreams.Close()
		defer streamBackupSaver.Close()
	}

	// we create a ComsoDB handler
	repo := cosmos.NewRepository(instance)

	// this is to track duplicates in any in the import file (duplicate streams definition)
	sensorId := make(map[string]int)

	// nice stuff for the user like a progress bar
	lineNumber, err := reader.CountLines()
	bar, bucket, remainder := progressBar(lineNumber, "Processing file "+file)
	defer bar.Finish()

	// We skip the first line (header)
	err = reader.SkipLine()
	if err != nil {
		LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: "Failed to skip header line"})
		return
	}

	for i := 2; ; i++ {
		if i%bucket == 0 {
			_ = bar.Add(bucket)
		}
		newStream := regEle.NewElement(user)
		//newStream := stream.NewStream(user)
		err = reader.ReadNext(newStream)
		if err != nil {
			if err == io.EOF {
				break
			}
			LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Failed to read stream line: %d in file: %s", i, file)})
			continue
		}
		// check is a row had the same sensorId we already processed in the file
		// SensorID is the primaryKey
		if _, ok := sensorId[newStream.GetID()]; ok {
			LogRecords = append(LogRecords, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[newStream.GetID()])})
			continue
		} else {
			sensorId[newStream.GetID()] = i
		}
		// if we know what we are doing, we can skip the check, useful for importing large number of new streams

		// fetchStreams in DB for the stream we just created, is the stream already existing?
		//fetchedStreams, err = repo.GetStreamByStreamIdAndSiteCode(newStream.GetID(), newStream.GetSiteCode())
		fetchedStreams, err = regEle.GetElementFromRepo(repo, newStream.GetID(), newStream.GetSiteCode())
		if err != nil {
			LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: "Failed to get stream"})
			continue
		}
		// if debug we save the stream we fetched
		if debug {
			// because we can have multiple streamed fetched
			for _, fetchedStream := range fetchedStreams {
				streamJson, _ := json.Marshal(fetchedStream)
				_ = streamBackupSaver.Write(streamJson)
			}
		}

		// we found multiple stream with the same SensorID this should not append...
		if len(fetchedStreams) > 1 {
			LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: fmt.Sprintf("More than one stream found in the Registry for \"%s\" at line %d in file %s the first stream will be updated", newStream.GetID(), i, file)})
			if debug {
				for _, fetchedStream := range fetchedStreams {
					LogRecords = append(LogRecords, internal.LogRecord{Err: errors.New("multiple stream defined"), Msg: fmt.Sprintf("sensorId: \"%s\" stream.ID: \"%s\"", fetchedStream.GetID(), fetchedStream.GetInternalID())})
				}
			}
		}

		// if the stream exists, we might need to update it
		// if more than one stream exist we update/modify the first one
		if len(fetchedStreams) == 1 || len(fetchedStreams) > 1 {
			// if we have more stream (which we should not) we delete the last one

			fetchedStreams[len(fetchedStreams)-1].Delete(user)

			// keep track if we had error when saving the batch
			unprocessedItems, logErr := repo.UpdateBatchedStreamsByStreamKey(fetchedStreams[len(fetchedStreams)-1])
			LogRecords = append(LogRecords, logErr...)

			// as we process by batch, we might have more than one stream unprocessed
			if len(unprocessedItems) > 0 {
				_ = persisteUnprocessedStreams.AddRows(itemsToRowList(unprocessedItems))
			}

			if debug {
				// save the stream uuid for debug
				_ = debugHandlerUpdatedStreams.Write(fetchedStreams[0].GetInternalID())
			}

		}
	}
	_ = bar.Add(remainder)
	fmt.Println()
	// Empty all Repo batches
	unprocessedItems, logErr := repo.Close()
	if len(unprocessedItems) > 0 {
		_ = persisteUnprocessedStreams.AddRows(itemsToRowList(unprocessedItems))
	}
	LogRecords = append(LogRecords, logErr...)

	if verb {
		internal.PrintLogRecord(LogRecords)
	}

}
