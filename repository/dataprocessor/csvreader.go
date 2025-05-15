// Package dataprocessor
// -----------------------------------------------------------------------------
// File: csvreader.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	The Main object that handles the CSV import file and creates Streams
//	It ensures the syntax of the CSV import file, and checks for mandatory
//	headers, any additional headers will be processed as FCTS Tags. No check
//	on Tags are performed, only a check is run for SAP Equipment Number Tags
//	It checks if the SAP Equipment Number is all digits.
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
	"errors"
	"fmi/stream-ingest/domain/base"
	"fmt"
	"io"
	"os"
)

const (
	ErrCSVReaderInvalidHeader = "invalid header"
	ErrCSVReaderMapper        = "mapper error"
	ErrCSVUnKnownTag          = "unknown tag"
)

var (
	UnknownTagErr = fmt.Errorf(ErrCSVUnKnownTag)
)

// Mapper defines an interface for processing key-value pairs where the key is a header and the value is a string.
type Mapper interface {
	Mapper(header string, value string) error
}

// CSVReader holds the CSV file, expected headers, and the CSV reader instance.
type CSVReader struct {
	file            *os.File
	reader          *csv.Reader
	user            string
	expectedHeaders []string
	headers         []string
}

// NewCSVReader initializes the CSVReader with an expected header format and opens the file.
func NewCSVReader(filePath string, user string, expectedHeaders []string) (*CSVReader, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, NewCSVReaderError("open file", err)
	}

	// Initialize CSVReader
	r := &CSVReader{
		file:            file,
		reader:          csv.NewReader(file),
		expectedHeaders: expectedHeaders,
		user:            user,
	}

	// Verify headers during initialization
	err = r.validateHeaders()
	if err != nil {
		_ = file.Close() // Close the file if header validation fails
		return nil, err
	}

	err = r.validateTags()
	if err != nil {
		if errors.Is(err, UnknownTagErr) {
			return r, err
		}
		_ = file.Close() // Close the file if header validation fails
		return nil, err
	}

	return r, nil
}

// GetHeaders returns the headers read from the CSV file as a slice of strings.
func (r *CSVReader) GetHeaders() []string {
	return r.headers
}

// validateHeaders reads the first line of the CSV and compares it with the expected headers.
func (r *CSVReader) validateHeaders() error {

	// Reset the reader to ensure we count lines from the beginning
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	// Read the headers
	headers, err := r.reader.Read()
	if err != nil {
		return NewCSVReaderError("failed to read headers", err)
	}
	r.headers = headers

	// CompareTo the headers with the expected ones
	if len(headers) < len(r.expectedHeaders) {
		return NewCSVReaderError("missing header", nil)
	}

	if !isSameHeaders(r.expectedHeaders, headers) {
		return NewCSVReaderError("unexpected header", fmt.Errorf(""))
	}
	// Remove BOM (0xEF 0xBB 0xBF if any, I guess MS excel UTF-8 stuff)
	/*
		for i, _ := range r.expectedHeaders {
			if len(headers[i]) >= 3 && headers[i][0] == 0xEF && headers[i][1] == 0xBB && headers[i][2] == 0xBF {
				headers[i] = headers[i][3:] // Remove BOM
			}
			if headers[i] != r.expectedHeaders[i] {
				return NewCSVReaderError("unexpected header", fmt.Errorf(" got '%s', want '%s'", headers[i], r.expectedHeaders[i]))
			}
		}
	*/

	// Reset reader again for subsequent operations
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	return nil
}

func (r *CSVReader) validateTags() error {
	// check tags
	var (
		errList   []error
		joinedErr error
	)
	// Reset the reader to ensure we count lines from the beginning
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	// Read the headers
	headers, err := r.reader.Read()
	if err != nil {
		return NewCSVReaderError("failed to read headers", err)
	}

	for i := len(r.expectedHeaders); i < len(headers); i++ {
		if !base.IsTag(headers[i]) {
			// Collect errors for invalid tags
			err := fmt.Errorf("'%s' is not a valid tag", headers[i])
			errList = append(errList, err)
		}

	}

	if errList != nil {
		errList = append(errList, UnknownTagErr)
		joinedErr = errors.Join(errList...)
	}

	// Reset reader again for subsequent operations
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	return joinedErr
}

// SkipLine skips the current line in the CSV file and advances the reader to the next line; returns io.EOF if it is the end of the file is reached.
func (r *CSVReader) SkipLine() error {
	_, err := r.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF // End of file
		}
		return NewCSVReaderError("failed to read row: ", err)
	}
	return nil
}

// ReadNext reads the next row and returns it as a Stream object or an error.
func (r *CSVReader) ReadNext(item Mapper) error {
	// Read the next record
	row, err := r.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF // End of file
		}
		return NewCSVReaderError("failed to read row: ", err)
	}

	// Convert the row into an Item
	err = parseRowToMapper(r.headers, row, item)
	if err != nil {
		return NewCSVReaderError("failed to parse row into Item ", err)
	}

	return nil
}

// CountLines returns the number of lines in the CSV file (excluding the header row).
func (r *CSVReader) CountLines() (int, error) {
	// Reset reader to ensure we count lines from the beginning
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return 0, NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	// Skip the header line
	_, err := r.reader.Read()
	if err != nil {
		return 0, NewCSVReaderError("failed to read header", err)
	}

	// Count remaining lines
	lineCount := 0
	for {
		_, err := r.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // End of file
			}
			return 0, NewCSVReaderError("failed to read line", err)
		}
		lineCount++
	}

	// Reset reader again for subsequent operations
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return 0, NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	return lineCount, nil
}

// ReadNext reads the next row and returns it as a Stream object or an error.
func (r *CSVReader) ReadAtLine(line int, item Mapper) error {
	// Read the next record
	row, err := r.getLineAt(line)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF // End of file
		}
		return NewCSVReaderError("failed to read row: ", err)
	}

	// Convert the row into an Item
	err = parseRowToMapper(r.headers, row, item)
	if err != nil {
		return NewCSVReaderError("failed to parse row into Item ", err)
	}

	return nil
}

// getLineAt reads a specific line from the CSV file and returns it as a slice of strings.
// Line numbers start at 1 (after the header). Returns an error if the line doesn't exist.
func (r *CSVReader) getLineAt(lineNumber int) ([]string, error) {
	if lineNumber < 1 {
		return nil, fmt.Errorf("invalid line number: %d, line numbers start at 1", lineNumber)
	}

	// Reset the reader to ensure we start from the beginning
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return nil, NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	// Skip the header line
	_, err := r.reader.Read()
	if err != nil {
		return nil, NewCSVReaderError("failed to read header", err)
	}

	// Read lines until we reach the target line
	currentLine := 1
	for currentLine < lineNumber {
		_, err := r.reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("line number %d exceeds file length", lineNumber)
			}
			return nil, NewCSVReaderError("failed to read line", err)
		}
		currentLine++
	}

	// Read the target line
	row, err := r.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("line number %d exceeds file length", lineNumber)
		}
		return nil, NewCSVReaderError("failed to read line", err)
	}

	// Reset reader again for subsequent operations
	if _, err := r.file.Seek(0, io.SeekStart); err != nil {
		return nil, NewCSVReaderError("failed to reset file position", err)
	}
	r.reader = csv.NewReader(r.file)

	return row, nil
}

// parseRowToMapper maps a CSV row to a Mapper implementation based on headers and returns an error if mapping fails.
func parseRowToMapper(headers []string, row []string, item Mapper) error {
	var err error

	// Ensure the length of row matches or exceeds the headers
	if len(row) < len(headers) {
		return errors.New(ErrCSVReaderInvalidHeader)
	}

	// Map each known header to the corresponding field in the Item
	for i, header := range headers {

		err = item.Mapper(header, row[i])
		if err != nil {
			return errors.Join(errors.New(ErrCSVReaderMapper), err)
		}

	}
	return nil
}

// Close closes the CSV file.
func (r *CSVReader) Close() error {
	return r.file.Close()
}

// ---- helper function
func isSameHeaders(reference []string, header []string) bool {
	var (
		headereMap map[string]bool
	)

	headereMap = make(map[string]bool)
	for i, _ := range header {
		// Remove BOM (0xEF 0xBB 0xBF if any, I guess MS excel UTF-8 stuff)
		if len(header[i]) >= 3 && header[i][0] == 0xEF && header[i][1] == 0xBB && header[i][2] == 0xBF {
			header[i] = header[i][3:] // Remove BOM
		}
		headereMap[header[i]] = true
	}

	for _, key := range reference {
		if !headereMap[key] {
			return false
		}
	}
	return true
}
