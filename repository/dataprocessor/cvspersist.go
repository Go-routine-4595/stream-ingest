package dataprocessor

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"

	"githb.com/Go-routine-4595/stream-ingest/model"

	"github.com/rs/zerolog/log"
)

type CSVPersist struct {
	file *os.File
}

func NewCSVPersist(fileName string) (CSVPersist, error) {
	file, err := os.Create(fileName)
	if err != nil {
		log.Logger.Err(err).Msg("failed to create file")
		return CSVPersist{}, NewCSVReaderError("failed to create file", err)
	}
	return CSVPersist{file: file}, nil
}

func (p CSVPersist) Close() error {
	return p.file.Close()
}

func (p CSVPersist) Persist(items []model.Item) error {

	// Create a CSV writer
	writer := csv.NewWriter(p.file)
	defer writer.Flush()

	// Write the headers to the CSV file
	if err := writer.Write(expectedHeaders); err != nil {
		log.Logger.Err(err).Msg("failed to write headers")
		return NewCSVReaderError("failed to write headers", err)
	}

	// Write the data rows to the CSV file
	var rowError bool
	rowError = false
	for _, item := range items {
		row := itemToString(item)
		if err := writer.Write(row); err != nil {
			log.Logger.Err(err).Msgf("failed to write row: %s", row)
			rowError = true
		}
	}
	if rowError {
		return NewCSVReaderError("failed to write rows", nil)
	}
	return nil
}

func itemToString(item model.Item) []string {
	res := []string{}

	// Convert all fields of the struct to strings using reflection
	v := reflect.ValueOf(item)
	for i := 0; i < v.NumField(); i++ {
		res = append(res, fmt.Sprintf("%v", v.Field(i).Interface()))
	}

	return res
}
