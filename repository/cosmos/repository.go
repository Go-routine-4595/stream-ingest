package cosmos

import (
	"context"
	"encoding/json"
	"githb.com/Go-routine-4595/stream-ingest/domain/stream"
	"githb.com/Go-routine-4595/stream-ingest/internal"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/rs/zerolog/log"
)

type Repository struct {
	Client    *azcosmos.Client
	Container *azcosmos.ContainerClient

	streamsToCreate map[string]BatchProcessing
	streamsToUpdate map[string]BatchProcessing
	streamsToDelete map[string]BatchProcessing
}

func NewRespository() *Repository {
	// Create a credential
	cred, err := azcosmos.NewKeyCredential(accountKey)
	if err != nil {
		log.Logger.Fatal().Msgf("Failed to create credentials: %v", err)
	}

	// Create a Cosmos DB client
	client, err := azcosmos.NewClientWithKey(accountEndpoint, cred, nil)
	if err != nil {
		log.Logger.Fatal().Msgf("Failed to create Cosmos DB client: %v", err)
	}

	// Specify the database and container
	container, _ := client.NewContainer(databaseName, containerName)

	return &Repository{
		Client:          client,
		Container:       container,
		streamsToCreate: make(map[string]BatchProcessing),
		streamsToUpdate: make(map[string]BatchProcessing),
		streamsToDelete: make(map[string]BatchProcessing),
	}
}

// GetStreamByStreamIdAndSiteCode retrieves a stream from the repository using the provided stream ID. Returns the stream or an error.
func (r *Repository) GetStreamByStreamIdAndSiteCode(sensorId string, siteCode string) ([]stream.Stream, error) {
	// Query items (example query: SELECT * FROM c WHERE c.id = '1')
	// query := "SELECT * FROM c WHERE c.id = @id"
	query := "SELECT * FROM c WHERE c.sensorId = @id"
	//query := "SELECT * FROM c"
	params := []azcosmos.QueryParameter{
		{Name: "@id", Value: sensorId},
	}

	queryOptions := &azcosmos.QueryOptions{
		QueryParameters: params,
	}

	partitionKey := azcosmos.NewPartitionKeyString(siteCode)

	// Define a context
	ctx := context.TODO()

	pager := r.Container.NewQueryItemsPager(query, partitionKey, queryOptions)
	streams := make([]stream.Stream, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			log.Logger.Fatal().Msgf("Failed to query items: %v", err)
		}

		for _, item := range page.Items {
			var streamEl stream.Stream
			err = json.Unmarshal(item, &streamEl)
			if err != nil {
				log.Logger.Fatal().Msgf("Failed to unmarshal item: %v", err)
			}
			streams = append(streams, streamEl)
		}
	}
	return streams, nil
}

func (r *Repository) Close() ([]stream.Stream, []internal.LogRecord) {
	var (
		errs          []internal.LogRecord = make([]internal.LogRecord, 0)
		streamsIssues []stream.Stream      = make([]stream.Stream, 0)
	)

	if len(r.streamsToCreate) > 0 {
		for siteCode, _ := range r.streamsToCreate {
			resStreamIssues, resErrLog := r.executeCreateBatchedStreamsByStreamKey(siteCode)
			errs = append(errs, resErrLog...)
			streamsIssues = append(streamsIssues, resStreamIssues...)
		}
	}
	if len(r.streamsToUpdate) > 0 {
		for siteCode, _ := range r.streamsToUpdate {
			resStreamIssues, resErrLog := r.executeUpdateBatchedStreamsByStreamKey(siteCode)
			errs = append(errs, resErrLog...)
			streamsIssues = append(streamsIssues, resStreamIssues...)
		}
	}
	return streamsIssues, errs
}
