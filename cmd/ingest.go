// Package cmd
// -----------------------------------------------------------------------------
// File: ingest.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            It provides functionalities to interact with the user and
//	            verify the input streams definition file syntax, check if the
//				streams already exist, if yes raise error in a log file
//				import-result_<date>.csv. If the checks are successful, then
//				the stream will be easier created or updated based on the
//				import file.
//				Note: every streams attribute will be updated as follows:
//				- siteCode is not updated, as it is a partition key in Cosmos
//				- tag will be added to CosmosDB
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
	"fmi/stream-ingest/domain/base"
	"fmi/stream-ingest/internal"
	"fmi/stream-ingest/model"
	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"
	"fmt"
	"io"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// ingestCmd handles the "ingest" command
var ingestCmd = &cobra.Command{
	Use:   "ingest [file]",
	Short: "Ingest data from the given file into the database",
	Long: `Ingest data from the given file into the database.
input file format for new streams:
- SiteCode | SensorId | StreamName | Process | ScaleFactor | MinValue | MaxValue | LoLo | Lo | Hi | HiHi | Uom 
input file format for new constants:
- SiteCode | ConstantName | Value | Process | MinValue | MaxValue | LoLo | Lo | Hi | HiHi | Uom 
All numeric values are optional, and will be defaulted to 0 if not provided:
- ScaleFactor: 1.0
- MinValue: 0
- MaxValue: 0
- LoLo: 0
- Lo: 0
- Hi: 0
- HiHi: 0

Supported Tags:
CollarElevation | EquipmentClass| EquipmentComponent | EquipmentMeasurement
EquipmentName | EquipmentSubUnit | EquipmentType | EquipmentUnit | GaugeFactor
GPSLatitude | GPSLongitude | Interpolation | OpStatsLoader | SAPEquipmentID | SAPMeasurementID
SAPMeasurementType | SAPUOM | Scaling | SensorElevation | SIMS | System | UDE | Workflow
ZeroReading | SAPSiteCode | OEM | Severity

SiteCode is the 3 letters site code used in FCTS (BAG/MOR/...)
the --skip flag is optional, it is used to skip the check for existence of streams /!\`,
	Example: `ingest -u 12345678 ./data/streams.csv`,
	Args:    cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		user, _ := cmd.Flags().GetString("user")
		skip, _ := cmd.Flags().GetBool("skip")
		debug, _ := cmd.Flags().GetBool("debug")
		prod, _ := cmd.Flags().GetBool("prod")
		verb, _ := cmd.Flags().GetBool("verbose")
		repeat, _ := cmd.Flags().GetBool("repeat")
		exp, _ := cmd.Flags().GetBool("experimental")
		site := cmd.Flag("site").Value.String()

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

		if repeat {
			base.SetOEMTagInfo(true)
		}
		con, rep := base.GetOEMTagInfo()
		fmt.Printf("You are using the OEM tag with :\"%s\" constraint and a repeat set to: \"%t\" \n", con, rep)
		question := fmt.Sprintf("Your are about to ingest data from file into %s DB, are you sure? (y/N) : ", instance)
		if !isUserOk(question, "") {
			return
		}

		if skip {
			if !isUserOk("The stream will be ingested without checking if the stream already exists in the database, are you sure? (y/N) : ", "Skipping check for existence of streams!") {
				skip = false
			}
		}

		fmt.Printf("Ingesting data from file: %s\n", file)
		if skip {
			fmt.Println("Skipping check for existence of streams!")
		}

		if exp {
			fmt.Println("Experimental mode, will be removed in the future, use with caution")
			if site == "" {
				fmt.Println("experimental option need a site code")
				return
			}
			executeIngestAsynch(file, instance, site, user)

		} else {
			// Call your logic to ingest the data here
			executeIngest(file, user, skip, debug, instance, verb)
		}
	},
}

func init() {
	// Add the "ingest" command and define its flag
	ingestCmd.Flags().StringP("user", "u", "", "employee id")
	ingestCmd.Flags().BoolP("skip", "s", false, "skip check for existence of streams use only if you are sure the streams are NOT already in the database /!\\ ")
	ingestCmd.Flags().BoolP("debug", "d", false, "debug mode, will save all UUID (id) of updated streams, created streams in 2 different log files and backup the streams before updating them.")
	ingestCmd.Flags().BoolP("prod", "p", false, "ingest data into the production CosmosDB, default import in the Dev CosmosDB.")
	ingestCmd.Flags().BoolP("verbose", "v", false, "verbose mode, will print all log messages.")
	ingestCmd.Flags().BoolP("repeat", "r", false, "used to concatenate a OEM tag to the stream if the tag already exists in the stream (useful for importing large number of OEM tag exceeding the excel cell limit)")
	ingestCmd.Flags().BoolP("experimental", "e", false, "experimental asynch batch ingestion, will be removed in the future, use with caution")
	ingestCmd.Flags().String("site", "", "Site 3 letter code")
	// Mark the "user" flag as required
	err := ingestCmd.MarkFlagRequired("user")
	if err != nil {
		log.Logger.Err(err).Msg("Failed to mark the 'user' flag as required")
	}

	rootCmd.AddCommand(ingestCmd)
}

func executeIngest(file string, user string, skip bool, debug bool, instance string, verb bool) {
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
		if !skip {
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
			// check if the stream in CosmosDB is valid
			err = fetchedStreams[0].Validate()
			if err != nil {
				LogRecords = append(LogRecords, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Registry stream:\"%s\" in ComsosDB has duplicated tag", fetchedStreams[0].GetID())})
				log.Fatal().Msgf("stream not valide: %v, %+v: ", err, fetchedStreams[0])
			}

			if !fetchedStreams[0].CompareTo(newStream) {
				// update the fetched stream with the newStream (newStream is form the csv file).
				// We might have additional tags not present in the csv file
				fetchedStreams[0].UpdateWith(newStream, user)

				// keep track if we had error when saving the batch
				unprocessedItems, logErr := repo.UpdateBatchedStreamsByStreamKey(fetchedStreams[0])
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
			continue
		}
		// no stream exists, we simply we create it
		if len(fetchedStreams) == 0 {
			// we replace all filed we don't have value form the csv file with defaults

			newStream.ProcessNumericalValue()

			// keep track if we had error when saving the batch
			unprocessedItems, logErr := repo.CreatBatchedStreamsByStreamKey(newStream)
			LogRecords = append(LogRecords, logErr...)

			// as we process by batch, we might have more than one stream unprocessed
			if len(unprocessedItems) > 0 {
				_ = persisteUnprocessedStreams.AddRows(itemsToRowList(unprocessedItems))
			}

			if debug {
				// save the new stream uuid for debug
				_ = debugHandlerNewStreams.Write(newStream.GetInternalID())
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
