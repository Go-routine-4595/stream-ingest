// Package cmd
// -----------------------------------------------------------------------------
// File: helper.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				 into FCTS.
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
	"bufio"
	"fmi/stream-ingest/repository/cosmos"
	"fmt"
	"github.com/k0kubun/go-ansi"
	"github.com/schollz/progressbar/v3"
	"math"
	"os"
	"strings"
	"time"
)

func progressBar(total int, text string) (*progressbar.ProgressBar, int, int) {
	bar := progressbar.NewOptions(total,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()), //you should install "github.com/k0kubun/go-ansi"
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(20),
		progressbar.OptionShowCount(),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan][1][reset] %s ...", text)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	bucketSize, remainder := bucketSizeAndRemainder(total)
	return bar, bucketSize, remainder
}

// bucketSizeAndRemainder calculates the most significant digit and the remainder for the number of element we want
// to process, it an optimization for the progress bar.
func bucketSizeAndRemainder(number int) (int, int) {
	dGroup := digitGroup(number)
	if dGroup > 3 {
		dGroup = 2
	} else {
		if dGroup == 0 {
			return 1, 0
		}
		dGroup -= 1
	}
	n := math.Pow(10, float64(dGroup))
	return int(number / int(n)), int(number % int(n))
}

// digitGroup determines the group of an integer based on its number of digits.
func digitGroup(number int) int {
	if number < 0 {
		number = -number // Make the number positive
	}
	if number == 0 {
		return 0
	}
	return int(math.Floor(math.Log10(float64(number))))
}

// itemsToRowList converts a slice of streams into a 2D slice of strings by flattening each stream with its tags.
// it takes a stream or a constant and returns a CSV []rows of []string
func itemsToRowList(elements []cosmos.Batcher) [][]string {
	var items [][]string
	for _, ele := range elements {
		items = append(items, ele.ToRow())
	}
	return items
}

// getCurrentTimestamp returns the current date and time in the format YYYYMMDDHHMMSS.
func getCurrentTimestamp() string {
	return time.Now().Format("20060102150405")
}

func CreateFileName(file string) string {
	return file + "_" + getCurrentTimestamp()
}

func getFileNameCsv(file string) string {
	fileName := fileExtension(file)
	return CreateFileName(fileName) + ".csv"
}
func getFileNameJson(file string) string {
	fileName := fileExtension(file)
	return CreateFileName(fileName) + ".jsonl"
}

func fileExtension(file string) string {
	s := strings.Split(file, ".")
	var i int
	if len(s) == 1 {
		i = 0
	} else {
		i = 1
	}
	return strings.Join(s[:len(s)-i], ".")
}
func isUserOk(question string, warining string) bool {
	reader := bufio.NewReader(os.Stdin)
	// Ask for acknowledge
	fmt.Print(question)
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(answer)
	if answer != "y" && answer != "Y" {
		return false
	} else {
		// Create a new ANSI stdout writer
		writer := ansi.NewAnsiStdout()
		fmt.Fprintf(writer, "\033[1;31m%s\033[0m\n", warining)
		return true
	}
}
