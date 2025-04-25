package internal

import (
	"bufio"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
)

const (
	begin = "{\"streamIds\": ["
	end   = "]}"
)

type DebugData struct {
	file       *os.File
	fileName   string
	buffer     *bufio.Writer
	bufferSize int
	hasElement bool
}

func NewDebugData(fileName string, bufferSize int) (*DebugData, error) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Logger.Err(err).Msg("failed to create file")
		return &DebugData{}, fmt.Errorf("failed to create file %w", err)
	}
	b := bufio.NewWriterSize(file, bufferSize)
	l, err := b.WriteString(begin)
	if err != nil {
		log.Logger.Err(err).Msg("failed to write to file")
		return &DebugData{}, fmt.Errorf("failed to write to file %w", err)
	}
	if l != len(begin) {
		log.Logger.Err(err).Msg("failed to write to file")
		return &DebugData{}, fmt.Errorf("failed to write to file %w", err)
	}
	return &DebugData{
		file:       file,
		fileName:   fileName,
		buffer:     b,
		bufferSize: bufferSize,
		hasElement: false,
	}, nil
}

// Write writes data to the buffer. If the buffer exceeds its limit, it automatically flushes to the file.
func (d *DebugData) Write(data string) error {
	var err error

	dataPoint := fmt.Sprintf("\"%s\"", data)
	if d.hasElement {
		_, err = d.buffer.WriteString("," + dataPoint)
	} else {
		_, err = d.buffer.WriteString(dataPoint)
		d.hasElement = true
	}

	if err != nil {
		log.Logger.Err(err).Msg("failed to write to buffer")
		return fmt.Errorf("failed to write to buffer: %w", err)
	}

	return nil
}

// Flush forces any buffered data to be written to the file.
func (d *DebugData) Flush() error {
	err := d.buffer.Flush()
	if err != nil {
		log.Logger.Err(err).Msg("failed to flush buffer")
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	return nil
}

// Close closes the underlying file and ensures all buffered data is flushed.
func (d *DebugData) Close() error {
	_, _ = d.buffer.WriteString(end)
	err := d.Flush()
	if err != nil {
		return err
	}

	err = d.file.Close()
	if err != nil {
		log.Logger.Err(err).Msg("failed to close file")
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
