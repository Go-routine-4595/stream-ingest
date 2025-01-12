package cmd

import (
	"fmt"
	"githb.com/Go-routine-4595/stream-ingest/model"
	"io"
	"os"
	"time"

	"githb.com/Go-routine-4595/stream-ingest/domain/stream"
	"githb.com/Go-routine-4595/stream-ingest/repository/cosmos"
	"githb.com/Go-routine-4595/stream-ingest/repository/dataprocessor"

	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

// ingestCmd handles the "ingest" command
var ingestCmd = &cobra.Command{
	Use:   "ingest [file]",
	Short: "Ingest data from the given file into the database",
	Args:  cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		update, _ := cmd.Flags().GetBool("update") // Get the value of the "update" flag
		user, _ := cmd.Flags().GetString("user")
		fmt.Printf("Ingesting data from file: %s\n", file)
		if update {
			fmt.Println("Update flag is set: Updating existing data in the database.")
		} else {
			fmt.Println("Ingesting new data only.")
		}
		// Call your logic to ingest the data here
		executeIngest(file, update, user)
	},
}

func init() {
	// Add the "ingest" command and define its flag
	ingestCmd.Flags().Bool("update", false, "Update existing data in the database")
	ingestCmd.Flags().StringP("user", "u", "", "employee id")

	// Mark the "user" flag as required
	err := ingestCmd.MarkFlagRequired("user")
	if err != nil {
		log.Logger.Err(err).Msg("Failed to mark the 'user' flag as required")
	}

	rootCmd.AddCommand(ingestCmd)
}

func executeIngest(file string, update bool, user string) {
	var (
		err                error
		newStream          *stream.Stream
		fetchedStreams     []stream.Stream
		unprocessedStreams []stream.Stream
		streamsToCreate    []stream.Stream
		streamsToUpdate    []stream.Stream
		reader             *dataprocessor.CSVReader
		repo               cosmos.Repository
		persite            dataprocessor.CSVPersist
		lineNumber         int
		bar                *progressbar.ProgressBar
		LogRecords         []logRecord
		sensorId           map[string]int
		resFile            string
	)

	reader, err = dataprocessor.NewCSVReader(file, user)
	if err != nil {
		log.Logger.Err(err)
		return
	}

	resFile = getFileName("import-result")
	persite, err = dataprocessor.NewCSVPersist(resFile)
	if err != nil {
		log.Logger.Err(err)
		return
	}

	defer reader.Close()
	defer persite.Close()

	repo = cosmos.NewRespository()

	unprocessedStreams = make([]stream.Stream, 0)
	streamsToCreate = make([]stream.Stream, 0)
	streamsToUpdate = make([]stream.Stream, 0)
	sensorId = make(map[string]int)

	lineNumber, err = reader.CountLines()
	bar = progressBar(lineNumber, "Processing file "+file+"...")
	defer bar.Finish()

	//Skipe the first line (header)
	_, _ = reader.ReadNext()

	for i := 2; ; i++ {
		bar.Add(1)
		newStream, err = reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			LogRecords = append(LogRecords, logRecord{err: err, msg: fmt.Sprintf("Failed to read next stream line: %d", i)})
			return
		}
		// check is a row had the same sensorId we already processed in the file
		// SensorID is the primaryKey
		if _, ok := sensorId[newStream.SensorID]; ok {
			LogRecords = append(LogRecords, logRecord{err: nil, msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[newStream.SensorID])})
			continue
		} else {
			sensorId[newStream.SensorID] = i
		}
		// fetchStreams in DB for the stream we just created, is the stream already existing?
		fetchedStreams, err = repo.GetStreamByStreamIdAndSiteCode(newStream.SensorID, newStream.SiteCode)
		if err != nil {
			LogRecords = append(LogRecords, logRecord{err: err, msg: "Failed to get stream"})
			unprocessedStreams = append(unprocessedStreams, *newStream)
			continue
		}
		// we found multiple steram with the same SensorID this should not append...
		if len(fetchedStreams) > 1 {
			LogRecords = append(LogRecords, logRecord{err: err, msg: fmt.Sprintf("More than one stream found in the Registry for %s at line %d in file %s", newStream.SensorID, i, file)})
			unprocessedStreams = append(unprocessedStreams, *newStream)
			continue
		}

		// if the stream exists we might need to update it
		if len(fetchedStreams) == 1 {
			if !stream.CompareStreams(fetchedStreams[0], *newStream) {
				LogRecords = append(LogRecords, logRecord{err: nil, msg: fmt.Sprintf("Registry streamId: %s need to be updated by file: %s row line: %d ", fetchedStreams[0].SensorID, file, i)})
				updateStream(update, &fetchedStreams[0], newStream, user)
				streamsToUpdate = append(streamsToUpdate, fetchedStreams[0])
			}
			continue
		}
		// no stream exist simple we create it
		if len(fetchedStreams) == 0 {
			streamsToCreate = append(streamsToCreate, *newStream)
		}
	}
	// Now we add our stream in the DB
	if len(streamsToCreate) > 0 {
		errs := repo.CreatStreamsByStreamKey(streamsToCreate)
		if len(errs) > 0 {
			addError(&LogRecords, errs, "failed to create stream")
		}
	}
	// Now we update stream in the DB
	if len(streamsToUpdate) > 0 {
		errs := repo.UpdateStreamsByStreamKey(streamsToUpdate)
		if len(errs) > 0 {
			addError(&LogRecords, errs, "failed to create stream")
		}
	}
	// if we have unpocessed stream we need to let know the user
	// with a new CSV file
	if len(unprocessedStreams) > 0 {
		processUnProcessed(persite, unprocessedStreams, &LogRecords)
	} else {
		_ = deleteFile(resFile)
	}
	printLogRecord(LogRecords)
}

func updateStream(update bool, stream1 *stream.Stream, stream2 *stream.Stream, user string) {
	if update {
		stream.UpdateStream(stream1, stream2, user)
	} else {
		stream.UpdateTags(stream1, stream2, user)
	}
}

func processUnProcessed(p dataprocessor.CSVPersist, unprocessed []stream.Stream, records *[]logRecord) {
	var items []model.Item

	items = make([]model.Item, len(unprocessed))

	for i, streamS := range unprocessed {
		item := streamS.ConvertStreamToItem()
		items[i] = item
	}
	err := p.Persist(items)
	if err != nil {
		*records = append(*records, logRecord{err: err, msg: "Failed to persist"})
	}

}

// getCurrentTimestamp returns the current date and time in the format YYYYMMDDHHMMSS.
func getCurrentTimestamp() string {
	return time.Now().Format("20060102150405")
}

func getFileName(file string) string {
	return file + "_" + getCurrentTimestamp() + ".csv"
}

// deleteFile removes the specified file and returns an error if it fails.
func deleteFile(filePath string) error {
	err := os.Remove(filePath)
	if err != nil {
		return fmt.Errorf("failed to delete file %s: %w", filePath, err)
	}
	return nil
}
