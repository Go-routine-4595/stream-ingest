package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "stream-ingest",
	Short: "Stream Ingest is a CLI tool for verifying, checking, and ingesting data into your database.",
}

func Execute() {
	// Execute the CLI
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
