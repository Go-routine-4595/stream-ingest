package cmd

import (
	"fmt"
	"github.com/k0kubun/go-ansi"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

type logRecord struct {
	err error
	msg string
}

func printLogRecord(logRcords []logRecord) {
	if len(logRcords) > 0 {
		for _, logR := range logRcords {
			log.Logger.Err(logR.err).Msgf("%s", logR.msg)
		}
	}
}

func addError(logRecords *[]logRecord, err []error, msg string) {
	for _, e := range err {
		*logRecords = append(*logRecords, logRecord{err: e, msg: msg})
	}
}

func progressBar(total int, text string) *progressbar.ProgressBar {
	bar := progressbar.NewOptions(total,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()), //you should install "github.com/k0kubun/go-ansi"
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription(fmt.Sprintf("[cyan][1/3][reset] %s ...", text)),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	return bar
}
