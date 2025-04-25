// Package cmd
// -----------------------------------------------------------------------------
// File: root.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	             It provides functionalities to interact with the user and
//	             process/input/output data accordingly.
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
	"fmt"
	"os"

	"fmi/stream-ingest/config"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "stream-ingest",
	Short:   "Stream Ingest is a CLI tool for verifying, checking, and ingesting data into your database.",
	Version: config.Version + " -- BuildDate: " + config.BuildDate,
	Long: `Stream Ingest is a CLI tool for verifying, checking, and ingesting data into your database.
input file format for new streams:
- SiteCode | Process | StreamName | SensorId | Uom | ScaleFactor | MinValue | MaxValue | LoLo | Lo | Hi | HiHi
input file format for new constants:
- SiteCode | Process | StreamName | Value | Uom | MinValue | MaxValue | LoLo | Lo | Hi | HiHi`,

	Run: func(cmd *cobra.Command, args []string) {
		val, _ := cmd.Flags().GetBool("version")
		if val {
			fmt.Println("Stream Ingest Version: " + config.Version + "." + config.BuildDate)
		}
		fmt.Println("Stream Ingest Version: " + config.Version + "." + config.BuildDate)
		fmt.Println("Author: " + config.Author)
		fmt.Println("Email: " + config.Email)
		fmt.Println("Date: " + config.BuildDate)
		fmt.Println(cmd.Long)
	},
}

func Execute() {
	// Execute the CLI
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
