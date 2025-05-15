// Package cmd
// -----------------------------------------------------------------------------
// File: resynchronize.go
// Description: This file implements the CLI command(s) for ingesting stream
//				Resynchronize Registry CosmosDB from a reference CSV file
//
// Author: <Christophe Buffard>
// Created: <01/17/2025>
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
	"fmt"
	"io"
	"strings"

	"fmi/stream-ingest/domain/stream"
	"fmi/stream-ingest/internal"
	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"

	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// ingestCmd handles the "ingest" command
var resynchronizeCmd = &cobra.Command{
	Use:   "resynchronize [file]",
	Short: "Resynchronize data from the given file into the database",
	Long: `Resynchronize data from the given file into the database.
input file format:
- SiteCode | Process | StreamName | SensorId | Uom | ScaleFactor| MinValue | MaxValue | LoLo | Lo | Hi | HiHi
All numeric values are optional, and will be defaulted to 0 if not provided:
- ScaleFactor: 1.0
- MinValue: 0
- MaxValue: 0
- LoLo: 0
- Lo: 0
- Hi: 0
- HiHi: 0
SiteCode is the 3 letters sitecode used in FCTS (BAG/MOR/...)
This command will delete all tags presents in CosmosDB and will create new ones based on the import file.
the --user flag is mandatory, it is used to identify the user who is ingesting the data, 
the --skip flag is optional, it is used to skip the check for existence of streams /!\`,
	Example: `resynchronize -u 123456789 ./data/streams.csv`,
	Args:    cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		user, _ := cmd.Flags().GetString("user")
		skip, _ := cmd.Flags().GetBool("skip")
		debug, _ := cmd.Flags().GetBool("debug")
		prod, _ := cmd.Flags().GetBool("prod")

		if len(user) < 10 {
			// if the user is less than 10 characters we are padding with 0
			user = strings.Repeat("0", 10-len(user)) + user
		}

		var instance string
		if prod {
			instance = "Prod"
		} else {
			instance = "Dev"
		}

		question := fmt.Sprintf("Your are about to synchronize data from file into %s DB, are you sure? (y/N) : ", instance)
		if isUserOk(question, "") {
			return
		}

		if skip {
			if isUserOk("The stream will be resynchronized without checking if the stream already exists in the database, are you sure? (y/N) : ", "Skipping check for existance of streams!") {
				skip = false
			}
		}

		fmt.Printf("Ingesting data from file: %s\n", file)

		// Call your logic to ingest the data here
		executeResynchronize(file, user, skip, debug, instance)
	},
}

func init() {
	// Add the "ingest" command and define its flag
	resynchronizeCmd.Flags().StringP("user", "u", "", "employee id")
	resynchronizeCmd.Flags().BoolP("skip", "s", false, "skip check for existance of streams use only if you are sure the streams are NOT already in the database")
	resynchronizeCmd.Flags().BoolP("debug", "d", false, "debug mode, will save all UUID (id) of updated streams in the log file.")
	resynchronizeCmd.Flags().BoolP("prod", "p", false, "Production CosmosDB used.")
	// Mark the "user" flag as required
	err := resynchronizeCmd.MarkFlagRequired("user")
	if err != nil {
		log.Logger.Err(err).Msg("Failed to mark the 'user' flag as required")
	}

	rootCmd.AddCommand(resynchronizeCmd)
}

func executeResynchronize(file string, user string, skip bool, debug bool, instance string) {
	var (
		err                        error
		newStream                  *stream.Stream
		fetchedStreams             []stream.Stream
		reader                     *dataprocessor.CSVReader
		repo                       *cosmos.Repository
		persiteUnprocessedStreams  *dataprocessor.CSVPersist
		lineNumber                 int
		bar                        *progressbar.ProgressBar
		LogRecords                 []internal.LogRecord
		sensorId                   map[string]int
		resFile                    string
		debugHandlerUpdatedStreams *internal.DebugData
	)

	// we create a reader for the input CSV file
	reader, err = dataprocessor.NewCSVReader(file, user, stream.GetExpectedHeader())
	if err != nil {
		if errors.Is(err, dataprocessor.UnknownTagErr) {
			log.Logger.Err(err).Msg("unknown tag in the CSV file")
			if !isUserOk("Do you want to continue anyway? (y/N) : ", "new tagset will be created") {
				return
			}
		} else {
			log.Logger.Err(err).Msg("failed to create reader")
			return
		}
	}

	// we create a writer to persit the issue we have encounter so we know what has not been created
	resFile = getFileNameCsv("import-result")
	persiteUnprocessedStreams, err = dataprocessor.NewCSVPersist(resFile, reader.GetHeaders(), 4096)
	if err != nil {
		log.Logger.Err(err)
		return
	}

	defer reader.Close()
	defer persiteUnprocessedStreams.Close()

	// for a debug mode we store all UUID of the stream we are creating, in case we need to clean that mess up easily :)
	if debug {
		debugHandlerUpdatedStreams, err = internal.NewDebugData("stream-id-updated-"+getCurrentTimestamp()+".json", 4096)
		if err != nil {
			log.Logger.Err(err).Msg("failed to create debug data")
			return
		}

		defer debugHandlerUpdatedStreams.Close()
	}

	// we create a ComsoDB handler
	repo = cosmos.NewRepository(instance)

	// this is to track duplicates in any in the import file (duplicate streams definition)
	sensorId = make(map[string]int)

	// nice stuff for the user like a progress bar
	lineNumber, err = reader.CountLines()
	bar, _, _ = progressBar(lineNumber, "Processing file "+file)
	defer bar.Finish()

	// We skip the first line (header)
	err = reader.SkipLine()
	if err != nil {
		LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: "Failed to skip header line"})
		return
	}

	for i := 2; ; i++ {
		_ = bar.Add(1)
		newStream = stream.NewStream(user)
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
		if _, ok := sensorId[newStream.SensorID]; ok {
			LogRecords = append(LogRecords, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[newStream.SensorID])})
			continue
		} else {
			sensorId[newStream.SensorID] = i
		}
		// if we know what we are doing we can skip the check, useful for importing large number of new streams
		if !skip {
			// fetchStreams in DB for the stream we just created, is the stream already existing?
			fetchedStreams, err = repo.GetStreamByStreamIdAndSiteCode(newStream.SensorID, newStream.SiteCode)
			if err != nil {
				LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: "Failed to get stream"})
				continue
			}
		}
		// we found multiple stream with the same SensorID this should not append...
		if len(fetchedStreams) > 1 {
			LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: fmt.Sprintf("More than one stream found in the Registry for \"%s\" at line %d in file %s the first stream will be updated", newStream.SensorID, i, file)})
			if debug {
				for _, fetchedStream := range fetchedStreams {
					LogRecords = append(LogRecords, internal.LogRecord{Err: errors.New("multiple stream defined"), Msg: fmt.Sprintf("sensorId: \"%s\" stream.ID: \"%s\"", fetchedStream.SensorID, fetchedStream.ID)})
				}
			}
			//_ = persiteUnprocessedStreams.AddRow(newStream.ToRow())

		}

		// if the stream exists, we might need to update it
		// if more than one stream exist we update/modify the first one
		if len(fetchedStreams) == 1 || len(fetchedStreams) > 1 {

			// check if the stream in CosmosDB is valid
			err = fetchedStreams[0].Validate()
			if err != nil {
				LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Registry stream:\"%s\" in ComsosDB has duplicated tag", fetchedStreams[0].SensorID)})
				continue
			}
			// update the fetched stream, we keep this one we might have additional tags not present in the
			// csv file
			stream.ResynchronizeStream(&fetchedStreams[0], newStream, user)

			// keep track if we had error when saving the batch
			unprocessedItems, logErr := repo.UpdateBatchedStreamsByStreamKey(&fetchedStreams[0])
			LogRecords = append(LogRecords, logErr...)

			// as we process by batch, we might have more than one stream unprocessed
			if len(unprocessedItems) > 0 {
				_ = persiteUnprocessedStreams.AddRows(itemsToRowList(unprocessedItems))
			}

			if debug {
				// save the stream uuid for debug
				_ = debugHandlerUpdatedStreams.Write(fetchedStreams[0].ID)
			}

			continue
		}
		// no stream exists we raise an error we should only resynchronize existing streams
		if len(fetchedStreams) == 0 {
			LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Stream not found in the Registry for \"%s\" at line %d in file %s", newStream.SensorID, i, file)})
			if debug {
				for _, fetchedStream := range fetchedStreams {
					LogRecords = append(LogRecords, internal.LogRecord{Err: errors.New("stream not found"), Msg: fmt.Sprintf("sensorId: \"%s\" stream.ID: \"%s\"", fetchedStream.SensorID, fetchedStream.ID)})
				}
			}
			_ = persiteUnprocessedStreams.AddRow(newStream.ToRow())

			continue
		}
	}
	fmt.Println()
	// Empty all Repo batches
	unprocessedItems, logErr := repo.Close()
	if len(unprocessedItems) > 0 {
		_ = persiteUnprocessedStreams.AddRows(itemsToRowList(unprocessedItems))
	}
	LogRecords = append(LogRecords, logErr...)

	internal.PrintLogRecord(LogRecords)
}
