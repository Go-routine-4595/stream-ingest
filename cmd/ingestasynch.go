package cmd

import (
	"context"
	"errors"
	"fmi/stream-ingest/model"
	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"
	"fmt"
	"github.com/k0kubun/go-ansi"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"io"
)

type SensorAtLine struct {
	updated    bool
	lineNumber int
}

// executeIngestAsynch processes a CSV file and updates or creates sensor records in the database
func executeIngestAsynch(file, instance, site, user string) {
	sensorIDMap := getSensorIDMap(file)

	// Initialize repository
	repo := cosmos.NewRepository(instance)
	defer func() {
		unprocessedItems, _ := repo.Close()
		handleUnprocessedItems(unprocessedItems)
	}()

	// Initialize data registry and reader
	data, reader, err := initializeDataAndReader(file, user)
	if err != nil {
		return
	}
	defer reader.Close()

	// Process existing data
	updatedItemCount, err := processExistingData(data, repo, reader, site, sensorIDMap, user)
	if err != nil {
		return
	}

	// Process new data
	createdItemCount, err := processNewData(data, reader, repo, sensorIDMap, user)
	if err != nil {
		return
	}

	fmt.Println()
	fmt.Printf("%d updated items -------- %d new items\n", updatedItemCount, createdItemCount)
}

// initializeDataAndReader creates and initializes the data registry and CSV reader
func initializeDataAndReader(file, user string) (*model.Registry, *dataprocessor.CSVReader, error) {
	data, err := model.NewRegistry(file)
	if err != nil {
		fmt.Printf("New registry error: %v. Valid types are: stream or constant\n", err)
		return nil, nil, err
	}

	dataHeader := data.GetHeaders()
	reader, err := dataprocessor.NewCSVReader(file, user, dataHeader)
	if err != nil {
		if errors.Is(err, dataprocessor.UnknownTagErr) {
			log.Logger.Err(err).Msg("Unknown tag in the CSV file")
			if !isUserOk("Do you want to continue anyway? (y/N) : ", "new tag will be created") {
				return nil, nil, err
			}
		} else {
			log.Logger.Err(err).Msg("Failed to create reader")
			return nil, nil, err
		}
	}

	return data, reader, nil
}

// processExistingData processes and updates existing data from the database
func processExistingData(data *model.Registry, repo *cosmos.Repository, reader *dataprocessor.CSVReader, site string,
	sensorIDMap map[string]SensorAtLine, user string) (int, error) {
	updatedItemCount := 0
	count := 0

	bar := createProgressBar("fetching data from CosmosDB")
	defer bar.Finish()

	ch := make(chan model.RegistryInterface, pageSize)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data.GetDataBatchBySideCode(repo, site, ch, ctx)

	for item := range ch {
		sensorDetail, ok := sensorIDMap[item.GetID()]
		if item.GetStatus() == "active" && ok {
			newElement := data.NewElement(user)
			err := reader.ReadAtLine(sensorDetail.lineNumber, newElement)

			// Update the sensor detail
			sensorDetail.updated = true
			sensorIDMap[item.GetID()] = sensorDetail

			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				log.Error().Msgf("Failed to read row: %v", err)
				return updatedItemCount, err
			}

			if !item.CompareTo(newElement) {
				item.UpdateWith(newElement, user)
				updatedItemCount++
				unprocessedItems, _ := repo.UpdateBatchedStreamsByStreamKey(item)
				if err := handleUnprocessedItems(unprocessedItems); err != nil {
					return updatedItemCount, err
				}
			}
		}

		count++
		if count%pageSize == 0 {
			_ = bar.Add(pageSize)
		}
	}

	_ = bar.Add(count % pageSize)
	return updatedItemCount, nil
}

// processNewData processes and creates new data entries
func processNewData(data *model.Registry, reader *dataprocessor.CSVReader,
	repo *cosmos.Repository, sensorIDMap map[string]SensorAtLine, user string) (int, error) {
	createdItemCount := 0

	for _, sensorDetail := range sensorIDMap {
		if sensorDetail.updated {
			continue
		}

		newElement := data.NewElement(user)
		err := reader.ReadAtLine(sensorDetail.lineNumber, newElement)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Error().Msgf("Failed to read row: %v", err)
			return createdItemCount, err
		}

		newElement.ProcessNumericalValue()
		unprocessedItems, _ := repo.CreatBatchedStreamsByStreamKey(newElement)
		createdItemCount++

		if err := handleUnprocessedItems(unprocessedItems); err != nil {
			return createdItemCount, err
		}
	}

	return createdItemCount, nil
}

// createProgressBar creates a standard progress bar with the given description
func createProgressBar(description string) *progressbar.ProgressBar {
	return progressbar.NewOptions(-1,
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionSetWidth(20),
		progressbar.OptionShowCount(),
		progressbar.OptionShowDescriptionAtLineEnd(),
		progressbar.OptionSetDescription(description),
	)
}

// handleUnprocessedItems handles any unprocessed items, logging them and returning an error if any exist
func handleUnprocessedItems(unprocessedItems []cosmos.Batcher) error {
	if len(unprocessedItems) > 0 {
		showUnprocessedData(unprocessedItems)
		log.Logger.Panic().Msg("unprocessed data")
		return errors.New("unprocessed data found")
	}
	return nil
}

// getSensorIDMap creates a map of sensor IDs to their positions in the CSV file
func getSensorIDMap(file string) map[string]SensorAtLine {
	sensorIDMap := make(map[string]SensorAtLine)
	regEle, err := model.NewRegistry(file)
	if err != nil {
		log.Logger.Panic().Msgf("unrecognized csv header: %v", err)
	}

	// Create a reader for the input CSV file
	reader, err := dataprocessor.NewCSVReader(file, "", regEle.GetHeaders())
	if err != nil {
		if !errors.Is(err, dataprocessor.UnknownTagErr) {
			log.Logger.Panic().Msgf("failed to create reader: %v", err)
		}
	}
	defer reader.Close()

	// Display progress bar
	lineNumber, err := reader.CountLines()
	bar, bucket, remainder := progressBar(lineNumber, "Processing file "+file)
	defer bar.Finish()

	// Skip the first line (header)
	err = reader.SkipLine()
	for i := 2; ; i++ {
		if i%bucket == 0 {
			_ = bar.Add(bucket)
		}

		newStream := regEle.NewElement("")
		err = reader.ReadNext(newStream)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Logger.Panic().Msgf("Failed to read stream line: %d in file: %s", i, file)
		}

		// i - 1: because i is not 0 indexed
		sensorIDMap[newStream.GetID()] = SensorAtLine{
			lineNumber: i - 1,
			updated:    false,
		}
	}

	_ = bar.Add(remainder)
	fmt.Println()
	return sensorIDMap
}

// showUnprocessedData logs information about unprocessed items
func showUnprocessedData(unprocessedItems []cosmos.Batcher) {
	for i := range unprocessedItems {
		log.Logger.Warn().Msgf("unprocessed data: %v", unprocessedItems[i].ToRow())
	}
}
