// Package cmd
// -----------------------------------------------------------------------------
// File: dump.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            Dump all streams in the CosmosDB for a site and save it in CVS file
//
// Author: <Christophe Buffard>
// Created: <03/25/2025>
// -----------------------------------------------------------------------------
// Notes:
//   - This file is part of the FCTS/stream ingestion project.
//   - Updated/reliable documentation and usage examples can be found at:
//     <Link to project README or documentation>
//
// -----------------------------------------------------------------------------
package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmi/stream-ingest/model"
	"fmt"
	"github.com/k0kubun/go-ansi"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"io"

	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"

	"github.com/spf13/cobra"
)

const (
	pageSize = 1000
)

// checkCmd handles the "check" command
var dumpCmd = &cobra.Command{
	Use:   "dump [file]",
	Short: "dump all steams for a site and save it as CSV file",
	Args:  cobra.ExactArgs(1), // Expect exactly one argument (file)
	Run: func(cmd *cobra.Command, args []string) {
		file := args[0]
		prod, _ := cmd.Flags().GetBool("prod")
		var instance string
		if prod {
			instance = "Prod"
		} else {
			instance = "Dev"
		}
		site := cmd.Flag("site").Value.String()
		dtype := cmd.Flag("type").Value.String()
		format, _ := cmd.Flags().GetBool("json")
		fmt.Printf("Dumping data in file %s exists in the database %s \n", file, instance)
		// Call your logic to check the file contents against the database here
		executeDump(file, instance, site, dtype, format)
	},
}

func init() {
	dumpCmd.Flags().BoolP("prod", "p", false, "Production CosmosDB used.")
	dumpCmd.Flags().StringP("type", "t", "stream", "dump stream or constants, default is stream. Possible values: stream, constants.")
	dumpCmd.Flags().StringP("site", "s", "", "Site 3 letter code")
	dumpCmd.Flags().BoolP("json", "j", false, "json file to save the stream definition. Possible values: stream, constants. Default is stream.")
	rootCmd.AddCommand(dumpCmd)
}

func executeDump(file string, instance string, site string, dtype string, formatJson bool) {
	var (
		err             error
		count           int
		saveSensorCount int
		outPutCsv       *dataprocessor.CSVPersist
		outPutJson      *dataprocessor.FileSaver
		fileName        string
	)

	repo := cosmos.NewRepository(instance)
	defer repo.Close()

	data, err := model.NewRegistryFromType(dtype)
	if err != nil {
		fmt.Printf("new registry from type error: %v  valid types are: stream or constant\n", err)
		return
	}

	dataHEader := data.GetHeaders()

	if formatJson {
		fileName = getFileNameJson(file)
		outPutJson, err = dataprocessor.NewFileSaver(fileName, 4096)
		if err != nil {
			fmt.Printf("error opening the file: %v \n", err)
			return
		}
		defer outPutJson.Close()
	} else {
		fileName = getFileNameCsv(file)
		outPutCsv, err = dataprocessor.NewCSVPersist(fileName, dataHEader, 10)
		if err != nil {
			fmt.Printf("unregonize csv header: %v \n", err)
			return
		}
		defer outPutCsv.Close()
	}

	//bar, bucket, remainder := prgressBarSimple(lineNumber, "Processing file "+file)
	bar := progressbar.NewOptions(-1,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionSetWidth(20),
		progressbar.OptionShowCount(),
		progressbar.OptionShowDescriptionAtLineEnd(),
		progressbar.OptionSetDescription("fetching data from CosmosDB"),
	)

	defer bar.Finish()

	ch := make(chan model.RegistryInterface, 100)
	ctx, cancel := context.WithCancel(context.Background())
	data.GetDataBatchBySideCode(repo, site, ch, ctx)
	for item := range ch {
		if item.GetStatus() == "active" {
			saveSensorCount++
			if formatJson {
				err = writeJson(outPutJson, item)
			} else {
				err = writeCsv(outPutCsv, item)
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Error().Msgf("Failed to write row: %v", err)
				cancel()
				return
			}
		}
		count++
		if count%pageSize == 0 {
			_ = bar.Add(pageSize)
		}

	}
	cancel()
	_ = bar.Add(count % pageSize)
	_ = bar.Finish()
	fmt.Println()
	fmt.Printf("Total number of streams saved: %d \n", saveSensorCount)

}

func writeCsv(outPutCsv *dataprocessor.CSVPersist, item model.RegistryInterface) error {
	row := item.ToRow()
	err := outPutCsv.AddRow(row)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}
		log.Error().Msgf("Failed to write row: %v", err)
		return err
	}
	return nil
}

func writeJson(outPutJson *dataprocessor.FileSaver, item model.RegistryInterface) error {
	data, err := json.Marshal(item)
	if err != nil {
		log.Error().Msgf("Failed to write row: %v", err)
		return err
	}
	err = outPutJson.Write(data)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return err
		}
		log.Error().Msgf("Failed to write row: %v", err)
		return err
	}
	return nil
}
