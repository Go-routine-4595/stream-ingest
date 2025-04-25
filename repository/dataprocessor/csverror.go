// Package dataprocessor
// -----------------------------------------------------------------------------
// File: csverror.go
// Description: This file implements the CLI command(s) for ingesting stream
//				CSV error helper
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

import "fmt"

// CSVReaderError represents a custom error type for the csvreader package.
type CSVReaderError struct {
	Op  string // Operation being performed, e.g., "open file"
	Err error  // The underlying error (optional)
}

func (e *CSVReaderError) Error() string {
	return fmt.Sprintf("csvreader error: %s: %v", e.Op, e.Err)
}

func (e *CSVReaderError) Unwrap() error {
	return e.Err
}

// NewCSVReaderError Helper to create a new CSVReaderError
func NewCSVReaderError(op string, err error) *CSVReaderError {
	return &CSVReaderError{Op: op, Err: err}
}
