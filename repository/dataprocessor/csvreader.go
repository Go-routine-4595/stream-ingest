package dataprocessor

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"githb.com/Go-routine-4595/stream-ingest/domain/stream"

	"githb.com/Go-routine-4595/stream-ingest/model"
)

// CSVReader holds the CSV file, expected headers, and the CSV reader instance.
type CSVReader struct {
	file            *os.File
	reader          *csv.Reader
	user            string
	expectedHeaders []string
	headers         []string
}

// ExpectedHeaders defines the list of strings representing the expected header names in a data processing context.
var expectedHeaders = []string{
	"SiteCode",
	"SensorID",
	"Name",
	"Process",
	"MinValue",
	"MaxValue",
	"Uom",
	"SiteShortCode",
	"System",
	"EquipmentUnit",
	"Subunit",
	"EquipmentComponent",
	"EquipmentMeasurement",
	"UDE",
	"SAP Equipment ID",
}

// NewCSVReader initializes the CSVReader with an expected header format and opens the file.
func NewCSVReader(filePath string, user string) (*CSVReader, error) {
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
		file.Close() // Close the file if header validation fails
		return nil, err
	}

	return r, nil
}

// validateHeaders reads the first line of the CSV and compares it with the expected headers.
func (r *CSVReader) validateHeaders() error {
	// Read the headers
	headers, err := r.reader.Read()
	if err != nil {
		return NewCSVReaderError("failed to read headers", err)
	}
	r.headers = headers

	// Compare the headers with the expected ones
	if len(headers) < len(r.expectedHeaders) {
		return NewCSVReaderError("missing header", nil)
	}

	for i := 0; i < len(r.expectedHeaders); i++ {
		//fmt.Println(headers[i], r.expectedHeaders[i])
		//fmt.Printf("headers[i]           %v \n", []byte(headers[i]))
		//fmt.Printf("r.expectedHeaders[i] %v \n", []byte(r.expectedHeaders[i]))
		if len(headers[i]) >= 3 && headers[i][0] == 0xEF && headers[i][1] == 0xBB && headers[i][2] == 0xBF {
			//fmt.Println("BOM detected")
			headers[i] = headers[i][3:] // Remove BOM
		}
		if headers[i] != r.expectedHeaders[i] {
			return NewCSVReaderError("unexpected header", fmt.Errorf(" got '%s', want '%s'", headers[i], r.expectedHeaders[i]))
		}
	}

	return nil
}

// ReadNext reads the next row and returns it as a Stream object or an error.
func (r *CSVReader) ReadNext() (*stream.Stream, error) {
	// Read the next record
	row, err := r.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF // End of file
		}
		return nil, NewCSVReaderError("failed to read row: %w", err)
	}

	// Convert the row into a Item struct
	item, err := parseRowToItem(expectedHeaders, row)
	if err != nil {
		return nil, NewCSVReaderError("failed to parse row into Item: %w", err)
	}
	// Convert an Item into a Stream structure
	streamRes, err := parseItemToStream(item, r.user)
	if err != nil {
		return nil, NewCSVReaderError("failed to parse row into Stream", err)
	}
	return streamRes, nil
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

// parseRowToItem converts a row (slice of strings) into a Stream struct.
func parseRowToItem(headers []string, row []string) (*model.Item, error) {
	// Ensure the length of row matches or exceeds the headers
	if len(row) < len(headers) {
		return nil, fmt.Errorf("row does not contain enough columns")
	}

	// Create the Item
	item := model.Item{}

	// A slice for unknown or extra tags
	var tags []model.Tag

	// Map each known header to the corresponding field in the Item
	for i, header := range headers {
		if i >= len(row) {
			break
		}
		ltag := model.Tag{}
		switch header {
		case "SiteCode":
			item.SiteCode = row[i]
		case "SensorID":
			item.SensorID = row[i]
		case "Name":
			item.Name = row[i]
		case "Process":
			// a hack for today
			if stream.IsProcess(row[i]) {
				item.Process = row[i]
			} else {
				item.Process = row[i]
			}
		case "MinValue":
			if row[i] == "" {
				row[i] = "0"
			}
			item.MinValue = row[i]
		case "MaxValue":
			if row[i] == "" {
				row[i] = "0"
			}
			item.MaxValue = row[i]
		case "Uom":
			item.UOM = row[i]
			// we expect only one SiteShortCode tag
		case "SiteShortCode":
			item.SiteShortCode = row[i]
			ltag.Name = "SiteShortCode"
			ltag.Value = row[i]
			tags = append(tags, ltag)
			// let's process system by a tag
		case "System":
			item.System = row[i]
			ltag.Name = "System"
			ltag.Value = row[i]
			tags = append(tags, ltag)
		case "EquipmentUnit":
			item.EquipmentUnit = row[i]
			// tag?
		case "Subunit":
			item.Subunit = row[i]
			ltags := processTags(header, row[i])
			tags = append(tags, ltags...)
			// can have multiple tag
		case "EquipmentComponent":
			item.EquipmentComponent = row[i]
			ltags := processTags(header, row[i])
			tags = append(tags, ltags...)
		case "EquipmentMeasurement":
			item.EquipmentMeasurement = row[i]
			ltag.Name = "EquipmentMeasurement"
			ltag.Value = row[i]
			tags = append(tags, ltag)
			// we may have multiple UDE
		case "UDE":
			item.UDE = row[i]
			ltags := processTags(header, row[i])
			tags = append(tags, ltags...)
		case "SAP Equipment ID":
			item.SAPEquipmentID = row[i]
			ltag.Name = "SAP Equipment ID"
			ltag.Value = row[i]
			tags = append(tags, ltag)
		default:
			if i >= len(expectedHeaders) {
				// we have process all known tags now it's unkonw tag we are processing
				// Add any unknown headers to the UnkownTag slice
				ltags := processTags(header, row[i])
				tags = append(tags, ltags...)
			}
		}
	}

	// Set any unknown tags in the item
	item.Tags = tags

	return &item, nil
}

func processTags(name string, value string) []model.Tag {
	split := strings.Split(value, ",")
	tags := make([]model.Tag, len(split))
	for i, tag := range split {
		tags[i] = model.Tag{
			Name:  name,
			Value: tag,
		}
	}
	return tags
}

func parseItemToStream(item *model.Item, user string) (*stream.Stream, error) {

	var err error

	// Creating a stream for the csv row representation
	streamRes := stream.NewStream()
	streamRes = streamRes.SetCreationBy(user)
	streamRes.SiteCode = item.SiteCode
	streamRes.Process = item.Process
	streamRes.StreamName = item.Name
	streamRes.SensorID = item.SensorID
	streamRes.UOM = item.UOM
	streamRes.Tags = make([]interface{}, len(item.Tags))
	for i, tag := range item.Tags {
		streamRes.Tags[i] = tag
	}

	streamRes.MinValue, err = strconv.Atoi(item.MinValue)
	if err != nil {
		return nil, errors.Join(errors.New("failed to get MinValue"), err)
	}
	streamRes.MaxValue, err = strconv.Atoi(item.MaxValue)
	if err != nil {
		return nil, errors.Join(errors.New("failed to get MaxValue"), err)
	}

	return &streamRes, nil
}

// Close closes the CSV file.
func (r *CSVReader) Close() error {
	return r.file.Close()
}
