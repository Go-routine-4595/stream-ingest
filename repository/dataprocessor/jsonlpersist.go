// Package dataprocessor
// -----------------------------------------------------------------------------
// File: jsonpersist.go
// Description: This file implements the CLI command(s) for ingesting stream
//				into FCTS.
//
//	            Persist data from a json marshall function. Maintain a buffer
//				which is flush regularly when it reaches bufferSize
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
	"os"
)

type FileSaver struct {
	file       *os.File
	fileName   string
	buffer     []byte
	bufferSize int // Maximum buffer size
}

// NewFileSaver initializes the object with a file name and buffer size
func NewFileSaver(fileName string, bufferSize int) (*FileSaver, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return &FileSaver{
		file:       f,
		buffer:     make([]byte, 0, bufferSize),
		bufferSize: bufferSize,
		fileName:   fileName,
	}, nil
}

func (fs *FileSaver) GetFileName() string {
	return fs.fileName
}

// Write adds data to the buffer and saves it to the file if the buffer is full
func (fs *FileSaver) Write(data []byte) error {
	// Append the newline to the data
	data = append(data, '\n')

	// Add the data to the buffer
	fs.buffer = append(fs.buffer, data...)

	// If the buffer exceeds the max size, flush it to the file
	if len(fs.buffer) >= fs.bufferSize {
		err := fs.flush()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close ensures all data in the buffer gets written to the file, then closes the file
func (fs *FileSaver) Close() error {
	// Flush any remaining buffer data to the file
	if len(fs.buffer) > 0 {
		if err := fs.flush(); err != nil {
			return err
		}
	}

	// Close the file
	return fs.file.Close()
}

// flush writes the buffer to the file and clears it
func (fs *FileSaver) flush() error {
	if _, err := fs.file.Write(fs.buffer); err != nil {
		return err
	}
	// Clear the buffer
	fs.buffer = fs.buffer[:0]
	return nil
}
