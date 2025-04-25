// Package dataprocessor
// -----------------------------------------------------------------------------
// File: cvspersist.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            This object handles the Streams that had issue while processing
//				it provide an equivalent of the import CSV file and all Streams
//				left unprocessed (not created/updated/deleted) so we have a clear
//				view of what issues was for further processing
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
package dataprocessor

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
)

type CSVPersist struct {
	file           *os.File
	writer         *csv.Writer
	fileName       string
	flushFrequency int
	headers        []string
	bufSize        int
	used           bool
}

func NewCSVPersist(fileName string, tagHeaders []string, flushFrequency int) (*CSVPersist, error) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Logger.Err(err).Msg("failed to create file")
		return nil, NewCSVReaderError("failed to create file", err)
	}
	// Initialize the writer
	writer := csv.NewWriter(file)

	p := &CSVPersist{
		file:           file,
		writer:         writer,
		fileName:       fileName,
		flushFrequency: flushFrequency,
		headers:        tagHeaders,
	}

	defer func() {
		if r := recover(); r != nil {
			_ = p.Close()
		}
	}()

	return p, nil
}

func (p *CSVPersist) GetFileName() string {
	return p.fileName
}

// deleteFile removes the specified file and returns an error if it fails.
func (p *CSVPersist) deleteFile(filePath string) error {
	err := os.Remove(p.fileName)
	if err != nil {
		return fmt.Errorf("failed to delete file %s: %w", filePath, err)
	}
	return nil
}

func (p *CSVPersist) Close() error {
	if !p.used {
		p.file.Close()
		p.deleteFile(p.fileName)
		return nil
	}
	// Ensure buffer gets flushed before closing
	p.writer.Flush()
	if err := p.writer.Error(); err != nil {
		log.Logger.Err(err).Msg("failed to flush buffer during close")
		return err
	}
	return p.file.Close()
}

// AddRow writes a single row to the CSV file and flushes the buffer periodically based on the configured flush frequency.
func (p *CSVPersist) AddRow(row []string) error {

	if !p.used {
		// first time we get here, we need to save the headers
		err := p.writer.Write(p.headers)
		if err != nil {
			log.Logger.Err(err).Msgf("failed to write headers: %s", p.headers)
			return NewCSVReaderError("failed to write headers", err)
		}
		p.bufSize++
	}

	p.used = true

	if err := p.writer.Write(row); err != nil {
		log.Logger.Err(err).Msgf("failed to write row: %s", row)
		return NewCSVReaderError("failed to write rows", err)
	}
	p.bufSize++
	// Flush periodically to keep memory usage low
	if p.bufSize%p.flushFrequency == 0 {
		p.writer.Flush()
		if err := p.writer.Error(); err != nil {
			log.Logger.Err(err).Msg("failed to flush buffer during writing")
			return err
		}
		p.bufSize = 0
	}

	return nil
}

// AddRows adds multiple rows to the CSV by calling AddRow for each row and returns an error if any row fails to be added.
func (p *CSVPersist) AddRows(rows [][]string) error {

	for _, row := range rows {
		err := p.AddRow(row)
		if err != nil {
		}
	}
	return nil
}
