package cmd

import (
	"fmt"
	"githb.com/Go-routine-4595/stream-ingest/repository/cosmos"
	"github.com/schollz/progressbar/v3"
	"io"

	"githb.com/Go-routine-4595/stream-ingest/domain/stream"
	"githb.com/Go-routine-4595/stream-ingest/repository/dataprocessor"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// checkCmd handles the "check" command
var checkCmd = &cobra.Command{
	Use:   "check [file]",
	Short: "Check if the data in the given file already exists in the database",
	Args:  cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		fmt.Printf("Checking if data in file %s exists in the database\n", file)
		// Call your logic to check the file contents against the database here
		executeCheck(file)
	},
}

func init() {
	rootCmd.AddCommand(checkCmd)
}

func executeCheck(file string) {
	var (
		err         error
		streamRes   *stream.Stream
		storedSteam []stream.Stream
		reader      *dataprocessor.CSVReader
		repo        cosmos.Repository
		lineNumber  int
		bar         *progressbar.ProgressBar
		logRecs     []logRecord
		sensorId    map[string]int
	)

	reader, err = dataprocessor.NewCSVReader(file, "")
	if err != nil {
		fmt.Println(err)
		return
	}

	defer reader.Close()

	repo = cosmos.NewRespository()
	sensorId = make(map[string]int)

	lineNumber, err = reader.CountLines()
	bar = progressBar(lineNumber, "Writing processing file "+file+"...")
	defer bar.Finish()

	// We skip the first line (header)
	_, _ = reader.ReadNext()
	for i := 2; ; i++ {
		bar.Add(1)
		streamRes, err = reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Logger.Err(err).Msg("Failed to read next stream")
			logRecs = append(logRecs, logRecord{err: err, msg: fmt.Sprintf("Failed to read next stream on line: %d", i)})
		}
		// check is a row had the same sensorId we already processed in the file
		// SensorID is the primaryKey
		if _, ok := sensorId[streamRes.SensorID]; ok {
			logRecs = append(logRecs, logRecord{err: nil, msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[streamRes.SensorID])})
			continue
		} else {
			sensorId[streamRes.SensorID] = i
		}
		// SensorID is the primaryKey
		storedSteam, err = repo.GetStreamByStreamIdAndSiteCode(streamRes.SensorID, streamRes.SiteCode)
		if err != nil {
			logRecs = append(logRecs, logRecord{err: err, msg: "Failed to get stream"})
			continue
		}
		if len(storedSteam) == 1 {
			if !stream.CompareStreams(storedSteam[0], *streamRes) {
				logRecs = append(logRecs, logRecord{err: nil, msg: fmt.Sprintf("Registry stream: %s need to be updated by file: %s row line: %d ", storedSteam[0].SensorID, file, i)})
			}

		}
		if len(storedSteam) > 1 {
			logRecs = append(logRecs, logRecord{err: err, msg: fmt.Sprintf("stream %s at line: %d  in file: %s appears more than once in the Registry", streamRes.SensorID, i, file)})
		}
	}
	fmt.Println("")
	printLogRecord(logRecs)
}
