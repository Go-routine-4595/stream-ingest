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
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		options := parseCommandOptions(cmd, args)

		if !confirmUserAction(options) {
			return
		}

		fmt.Printf("Ingesting data from file: %s\n", options.file)
		if options.skip {
			fmt.Println("Skipping check for existence of streams!")
		}

		if options.experimental {
			fmt.Println("Experimental mode, will be removed in the future, use with caution")
			if options.site == "" {
				fmt.Println("experimental option needs a site code")
				return
			}
			executeIngestAsynch(options)
		} else {
			executeIngest(options)
		}
	},
}

// CommandOptions holds all command line options
type IngestCommandOptions struct {
	file         string
	user         string
	skip         bool
	debug        bool
	instance     string
	verbose      bool
	site         string
	experimental bool
}

// IngestHandlers holds all file handlers used during ingestion
type IngestHandlers struct {
	persistedUnprocessed *dataprocessor.CSVPersist
	debugNewStreams      *internal.DebugData
	debugUpdatedStreams  *internal.DebugData
	streamBackupSaver    *dataprocessor.FileSaver
}

func parseCommandOptions(cmd *cobra.Command, args []string) IngestCommandOptions {
	user, _ := cmd.Flags().GetString("user")
	skip, _ := cmd.Flags().GetBool("skip")
	debug, _ := cmd.Flags().GetBool("debug")
	prod, _ := cmd.Flags().GetBool("prod")
	verb, _ := cmd.Flags().GetBool("verbose")
	repeat, _ := cmd.Flags().GetBool("repeat")
	exp, _ := cmd.Flags().GetBool("experimental")
	site := cmd.Flag("site").Value.String()

	// Ensure user ID is at least 10 characters
	if len(user) < 10 {
		user = strings.Repeat("0", 10-len(user)) + user
	}

	// Determine environment instance
	instance := "Dev"
	if prod {
		instance = "Prod"
	}

	// Handle OEM tag settings
	if repeat {
		base.SetOEMTagInfo(true)
	}
	con, rep := base.GetOEMTagInfo()
	fmt.Printf("You are using the OEM tag with :\"%s\" constraint and a repeat set to: \"%t\" \n", con, rep)

	return IngestCommandOptions{
		file:         args[0],
		user:         user,
		skip:         skip,
		debug:        debug,
		instance:     instance,
		verbose:      verb,
		site:         site,
		experimental: exp,
	}
}

func confirmUserAction(options IngestCommandOptions) bool {
	// Confirm main action
	question := fmt.Sprintf("Your are about to ingest data from file into %s DB, are you sure? (y/N) : ", options.instance)
	if !isUserOk(question, "") {
		return false
	}

	// Confirm skip option if selected
	if options.skip {
		if !isUserOk("The stream will be ingested without checking if the stream already exists in the database, are you sure? (y/N) : ",
			"Skipping check for existence of streams!") {
			options.skip = false
		}
	}

	return true
}

func init() {
	ingestCmd.Flags().StringP("user", "u", "", "employee id")
	ingestCmd.Flags().BoolP("skip", "s", false, "skip check for existence of streams use only if you are sure the streams are NOT already in the database /!\\ ")
	ingestCmd.Flags().BoolP("debug", "d", false, "debug mode, will save all UUID (id) of updated streams, created streams in 2 different log files and backup the streams before updating them.")
	ingestCmd.Flags().BoolP("prod", "p", false, "ingest data into the production CosmosDB, default import in the Dev CosmosDB.")
	ingestCmd.Flags().BoolP("verbose", "v", false, "verbose mode, will print all log messages.")
	ingestCmd.Flags().BoolP("repeat", "r", false, "used to concatenate a OEM tag to the stream if the tag already exists in the stream (useful for importing large number of OEM tag exceeding the excel cell limit)")
	ingestCmd.Flags().BoolP("experimental", "e", false, "experimental asynch batch ingestion, will be removed in the future, use with caution")
	ingestCmd.Flags().String("site", "", "Site 3 letter code")
	err := ingestCmd.MarkFlagRequired("user")
	if err != nil {
		log.Logger.Err(err).Msg("Failed to mark the 'user' flag as required")
	}
	rootCmd.AddCommand(ingestCmd)
}

func executeIngest(options IngestCommandOptions) {
	// Setup resources
	registry, reader, logRecords, handlers := setupIngestResources(options)
	if registry == nil || reader == nil {
		return
	}

	// Close resources when done
	defer closeResources(reader, handlers)

	// Process data
	processCsvData(registry, reader, options, handlers, logRecords)
}

// setupIngestResources initializes all necessary resources for ingestion
func setupIngestResources(options IngestCommandOptions) (*model.Registry, *dataprocessor.CSVReader,
	[]internal.LogRecord, *IngestHandlers) {

	var (
		err        error
		logRecords []internal.LogRecord
		handlers   = &IngestHandlers{}
	)

	// Initialize registry from file
	registry, err := model.NewRegistry(options.file)
	if err != nil {
		fmt.Printf("unrecognized csv header: %v \n", err)
		return nil, nil, logRecords, handlers
	}

	// Initialize CSV reader
	reader, err := dataprocessor.NewCSVReader(options.file, options.user, registry.GetHeaders())
	if err != nil {
		if errors.Is(err, dataprocessor.UnknownTagErr) {
			log.Logger.Err(err).Msg("unknown tag in the CSV file")
			if !isUserOk("Do you want to continue anyway? (y/N) : ", "new tag will be created") {
				return nil, nil, logRecords, handlers
			}
		} else {
			log.Logger.Err(err).Msg("failed to create reader")
			return nil, nil, logRecords, handlers
		}
	}

	// Setup result file for unprocessed streams
	resFile := getFileNameCsv("import-result")
	persisteUnprocessedStreams, err := dataprocessor.NewCSVPersist(resFile, reader.GetHeaders(), 4096)
	if err != nil {
		log.Logger.Err(err)
		return nil, nil, logRecords, handlers
	}
	handlers.persistedUnprocessed = persisteUnprocessedStreams

	// Setup debug handlers if debug mode is enabled
	if options.debug {
		if !setupDebugHandlers(handlers) {
			return nil, nil, logRecords, handlers
		}
	}

	return registry, reader, logRecords, handlers
}

// setupDebugHandlers initializes handlers for debug information
func setupDebugHandlers(handlers *IngestHandlers) bool {
	var err error

	// Setup handler for new streams
	handlers.debugNewStreams, err = internal.NewDebugData(getFileNameJson("stream-id-new"), 4096)
	if err != nil {
		log.Logger.Err(err).Msg("failed to create debug data")
		return false
	}

	// Setup handler for updated streams
	handlers.debugUpdatedStreams, err = internal.NewDebugData(getFileNameJson("stream-id-updated"), 4096)
	if err != nil {
		log.Logger.Err(err).Msg("failed to create debug data")
		return false
	}

	// Setup handler for stream backups
	handlers.streamBackupSaver, err = dataprocessor.NewFileSaver(getFileNameJson("streams-backup"), 4096)
	if err != nil {
		log.Logger.Err(err).Msg("failed to create debug data")
		return false
	}

	return true
}

// closeResources ensures all resources are properly closed
func closeResources(reader *dataprocessor.CSVReader, handlers *IngestHandlers) {
	reader.Close()

	if handlers.persistedUnprocessed != nil {
		handlers.persistedUnprocessed.Close()
	}

	if handlers.debugNewStreams != nil {
		handlers.debugNewStreams.Close()
	}

	if handlers.debugUpdatedStreams != nil {
		handlers.debugUpdatedStreams.Close()
	}

	if handlers.streamBackupSaver != nil {
		handlers.streamBackupSaver.Close()
	}
}

// processCsvData reads and processes each line from the CSV file
func processCsvData(registry *model.Registry, reader *dataprocessor.CSVReader,
	options IngestCommandOptions, handlers *IngestHandlers, logRecords []internal.LogRecord) {

	// Initialize repository and tracking
	repo := cosmos.NewRepository(options.instance)
	sensorIdMap := make(map[string]int)

	// Setup progress bar
	lineNumber, err := reader.CountLines()
	bar, bucket, _ := progressBar(lineNumber, "Processing file "+options.file)
	defer bar.Finish()

	// Skip header line
	err = reader.SkipLine()
	if err != nil {
		logRecords = append(logRecords, internal.LogRecord{Err: err, Msg: "Failed to skip header line"})
		return
	}

	// Process each line
	for i := 2; ; i++ {
		if i%bucket == 0 {
			_ = bar.Add(bucket)
		}

		// Process individual stream
		if !processStreamLine(registry, reader, repo, sensorIdMap, i, options, handlers, &logRecords) {
			break // End of file or fatal error
		}
	}
}

// processStreamLine processes a single stream from the CSV
func processStreamLine(registry *model.Registry, reader *dataprocessor.CSVReader,
	repo *cosmos.Repository, sensorIdMap map[string]int, lineNum int,
	options IngestCommandOptions, handlers *IngestHandlers, logRecords *[]internal.LogRecord) bool {

	// Create new stream element
	newStream := registry.NewElement(options.user)
	err := reader.ReadNext(newStream)
	if err != nil {
		if err == io.EOF {
			return false // End of file
		}
		*logRecords = append(*logRecords, internal.LogRecord{
			Err: err,
			Msg: fmt.Sprintf("Failed to read stream line: %d in file: %s", lineNum, options.file),
		})
		return true // Continue to next line
	}

	// Check for duplicate sensor IDs
	if existingLine, ok := sensorIdMap[newStream.GetID()]; ok {
		*logRecords = append(*logRecords, internal.LogRecord{
			Err: nil,
			Msg: fmt.Sprintf("Duplicate SensorID on line: %d and %d", lineNum, existingLine),
		})
		return true // Continue to next line
	}

	sensorIdMap[newStream.GetID()] = lineNum

	// Process the stream
	if !options.skip {
		processExistingStream(newStream, repo, registry, options, handlers, logRecords)
	} else {
		processNewStream(newStream, repo, handlers, logRecords)
	}

	return true
}

// processExistingStream handles updating of existing streams
func processExistingStream(stream model.RegistryInterface, repo *cosmos.Repository,
	registry *model.Registry, options IngestCommandOptions, handlers *IngestHandlers,
	logRecords *[]internal.LogRecord) {

	fetchedStreams, err := registry.GetElementFromRepo(repo, stream.GetID(), stream.GetSiteCode())
	if err != nil {
		*logRecords = append(*logRecords, internal.LogRecord{Err: err, Msg: "Failed to get stream"})
		return
	}

	// Backup streams in debug mode
	if options.debug && handlers.streamBackupSaver != nil {
		backupStreams(fetchedStreams, handlers.streamBackupSaver)
	}

	// Handle multiple streams found
	if len(fetchedStreams) > 1 {
		handleMultipleStreams(fetchedStreams, stream, options, logRecords)
	}

	// Update existing stream
	if len(fetchedStreams) >= 1 {
		updateExistingStream(fetchedStreams[0], stream, options.user, repo, handlers, logRecords)
		return
	}

	// Create new stream if none found
	processNewStream(stream, repo, handlers, logRecords)
}

// backupStreams saves stream data for debug purposes
func backupStreams(streams []model.RegistryInterface, saver *dataprocessor.FileSaver) {
	for _, stream := range streams {
		streamJson, _ := json.Marshal(stream)
		_ = saver.Write(streamJson)
	}
}

// handleMultipleStreams handles the case when multiple streams are found
func handleMultipleStreams(fetchedStreams []model.RegistryInterface, newStream model.RegistryInterface,
	options IngestCommandOptions, logRecords *[]internal.LogRecord) {

	*logRecords = append(*logRecords, internal.LogRecord{
		Err: nil,
		Msg: fmt.Sprintf("More than one stream found in the Registry for \"%s\" - the first stream will be updated",
			newStream.GetID()),
	})

	if options.debug {
		for _, fetchedStream := range fetchedStreams {
			*logRecords = append(*logRecords, internal.LogRecord{
				Err: errors.New("multiple stream defined"),
				Msg: fmt.Sprintf("sensorId: \"%s\" stream.ID: \"%s\"",
					fetchedStream.GetID(), fetchedStream.GetInternalID()),
			})
		}
	}
}

// updateExistingStream updates an existing stream with new data
func updateExistingStream(existingStream, newStream model.RegistryInterface, user string,
	repo *cosmos.Repository, handlers *IngestHandlers, logRecords *[]internal.LogRecord) {

	err := existingStream.Validate()
	if err != nil {
		*logRecords = append(*logRecords, internal.LogRecord{
			Err: err,
			Msg: fmt.Sprintf("Registry stream:\"%s\" in CosmosDB has duplicated tag", existingStream.GetID()),
		})
		log.Fatal().Msgf("stream not valid: %v, %+v: ", err, existingStream)
	}

	// Only update if different
	if !existingStream.CompareTo(newStream) {
		existingStream.UpdateWith(newStream, user)
		unprocessedItems, logErr := repo.UpdateBatchedStreamsByStreamKey(existingStream)
		*logRecords = append(*logRecords, logErr...)

		// Store unprocessed items
		if len(unprocessedItems) > 0 && handlers.persistedUnprocessed != nil {
			_ = handlers.persistedUnprocessed.AddRows(itemsToRowList(unprocessedItems))
		}

		// Save updated stream ID for debug
		if handlers.debugUpdatedStreams != nil {
			_ = handlers.debugUpdatedStreams.Write(existingStream.GetInternalID())
		}
	}
}

// processNewStream creates a new stream
func processNewStream(newStream model.RegistryInterface, repo *cosmos.Repository,
	handlers *IngestHandlers, logRecords *[]internal.LogRecord) {

	newStream.ProcessNumericalValue()
	unprocessedItems, logErr := repo.CreatBatchedStreamsByStreamKey(newStream)
	*logRecords = append(*logRecords, logErr...)

	// Store unprocessed items
	if len(unprocessedItems) > 0 && handlers.persistedUnprocessed != nil {
		_ = handlers.persistedUnprocessed.AddRows(itemsToRowList(unprocessedItems))
	}

	// Save new stream ID for debug
	if handlers.debugNewStreams != nil {
		_ = handlers.debugNewStreams.Write(newStream.GetInternalID())
	}
}
