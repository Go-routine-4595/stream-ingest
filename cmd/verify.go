package cmd

import (
	"fmt"
	"io"

	"githb.com/Go-routine-4595/stream-ingest/domain/stream"
	"githb.com/Go-routine-4595/stream-ingest/internal"
	"githb.com/Go-routine-4595/stream-ingest/repository/dataprocessor"

	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
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
		err        error
		streamRes  *stream.Stream
		reader     *dataprocessor.CSVReader
		issue      bool
		lineNumber int
		bar        *progressbar.ProgressBar
		logRecs    []internal.LogRecord
		sensorId   map[string]int
	)

	issue = false
	reader, err = dataprocessor.NewCSVReader(file, "")
	if err != nil {
		fmt.Println(err)
		return
	}

	sensorId = make(map[string]int)

	defer reader.Close()

	lineNumber, err = reader.CountLines()
	bar = progressBar(lineNumber, "Processing file "+file+"...")
	defer bar.Finish()

	//we skip the first line (header)
	_, _ = reader.ReadNext()
	for i := 2; ; i++ {
		bar.Add(1)
		streamRes, err = reader.ReadNext()
		if err != nil {
			if err == io.EOF {
				break
			}
			logRecs = append(logRecs, internal.LogRecord{Err: err, Msg: fmt.Sprintf("Failed to read next stream on line: %d", i)})
			issue = true
		}
		// check is a row had the same sensorId we already processed in the file
		// SensorID is the primaryKey
		if _, ok := sensorId[streamRes.SensorID]; ok {
			logRecs = append(logRecs, internal.LogRecord{Err: nil, Msg: fmt.Sprintf("Duplicate SensorID on line: %d  and  %d", i, sensorId[streamRes.SensorID])})
		} else {
			sensorId[streamRes.SensorID] = i
		}
	}
	if !issue {
		fmt.Println("")
		log.Logger.Info().Msg("Syntax is valid")
	}
	if len(logRecs) > 0 {
		internal.PrintLogRecord(logRecs)
	}
}
