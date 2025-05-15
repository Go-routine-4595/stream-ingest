// Package cosmos
// -----------------------------------------------------------------------------
// File: repository.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            Main function create the CosmosDB object and manges batch for
//				Created/Updated/Deleted streams and is able to generate
//				list of streams that was not successfully process
//				so we have trace of what were not processed
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
package cosmos

import (
	"context"
	"encoding/json"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"

	"fmi/stream-ingest/domain/constant"
	"fmi/stream-ingest/domain/stream"
	"fmi/stream-ingest/internal"

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

func NewRepository(instance string) *Repository {
	var (
		accountKey      string
		accountEndpoint string
		databaseName    string
		containerName   string
	)
	switch instance {
	case "DevP":
		accountKey = accountKeyP
		accountEndpoint = accountEndpointP
		databaseName = databaseNameP
		containerName = containerNameP
	case "Dev":
		accountKey = accountKeyDev
		accountEndpoint = accountEndpointDev
		databaseName = databaseNameDev
		containerName = containerNameDev
	case "Prod":
		accountKey = accountKeyProd
		accountEndpoint = accountEndpointProd
		databaseName = databaseNameProd
		containerName = containerNameProd
	}
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

// GetDataBatchBySideCode retrieves a batch of data filtered by the specified siteCode and data type asynchronously.
// It executes a query on the Cosmos DB container and sends the resulting items to the provided channel.
func (r *Repository) GetDataBatchBySideCode(siteCode string, ch chan<- []byte, dataT string, ctx context.Context) {
	query := "SELECT * FROM c WHERE c.registryType = @type"

	partitionKey := azcosmos.NewPartitionKeyString(siteCode)

	params := []azcosmos.QueryParameter{
		{Name: "@type", Value: dataT},
	}
	queryOptions := &azcosmos.QueryOptions{
		QueryParameters: params,
	}
	pager := r.Container.NewQueryItemsPager(query, partitionKey, queryOptions)
	go getPageData(pager, ch, ctx)

}

// getPageData retrieves and processes pages of data from a Cosmos DB query asynchronously, sending items to a channel.
// It iterates through the pager, handles errors, and manages context cancellation to ensure clean termination.
// The function closes the channel after all items have been sent or if the context is canceled.
func getPageData(pager *runtime.Pager[azcosmos.QueryItemsResponse], ch chan<- []byte, ctx context.Context) {

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			log.Logger.Fatal().Msgf("Failed to query items: %v", err)
			close(ch)
			return
		}
		for _, item := range page.Items {
			ch <- item
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
			}
		}
	}
	close(ch)
	return
}

// GetStreamBatchBySideCode retrieves a batch of streams filtered by the given site code and sends them to a channel.
// siteCode specifies the partition key used for filtering streams.
// ch is the channel to which the retrieved streams are sent.
// Returns an error if the operation fails.
func (r *Repository) GetStreamBatchBySideCode(siteCode string, ch chan<- stream.Stream) context.CancelFunc {

	query := "SELECT * FROM c WHERE c.registryType = 'stream'"

	partitionKey := azcosmos.NewPartitionKeyString(siteCode)

	//queryOptions := &azcosmos.QueryOptions{
	//	PageSizeHint: maxOperations,
	//}
	pager := r.Container.NewQueryItemsPager(query, partitionKey, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go getPage(pager, ch, ctx)

	return cancel
}

// getPage retrieves pages of items from a runtime.Pager and sends unmarshalled stream.Stream objects into a channel.
// pager is the runtime.Pager used to fetch the query results.
// ch is the channel to which retrieved stream.Stream objects are sent.
// ctx is the context used to manage request lifetimes and cancellations.
// Closes the channel after processing all pages or upon encountering an error. Logs errors and terminates on failure.
func getPage(pager *runtime.Pager[azcosmos.QueryItemsResponse], ch chan<- stream.Stream, ctx context.Context) {

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			log.Logger.Fatal().Msgf("Failed to query items: %v", err)
			close(ch)
			return
		}
		for _, item := range page.Items {
			var (
				streamEl stream.Stream
			)
			err = json.Unmarshal(item, &streamEl)
			if err != nil {
				log.Logger.Fatal().Msgf("Failed to unmarshal item: %v", err)
				close(ch)
				return
			}
			ch <- streamEl
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
			}
		}
	}
	close(ch)
	return
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

// GetConstantByNameAndSiteCode retrieves a stream from the repository using the provided stream ID. Returns the stream or an error.
func (r *Repository) GetConstantByNameAndSiteCode(sensorId string, siteCode string) ([]constant.Constant, error) {
	// Query items (example query: SELECT * FROM c WHERE c.id = '1')
	// query := "SELECT * FROM c WHERE c.id = @id"
	query := "SELECT * FROM c WHERE c.streamName = @id"
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
	consts := make([]constant.Constant, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			log.Logger.Fatal().Msgf("Failed to query items: %v", err)
		}

		for _, item := range page.Items {
			var constEl constant.Constant
			err = json.Unmarshal(item, &constEl)
			if err != nil {
				log.Logger.Fatal().Msgf("Failed to unmarshal item: %v", err)
			}
			consts = append(consts, constEl)
		}
	}
	return consts, nil
}

func (r *Repository) Close() ([]Batcher, []internal.LogRecord) {
	var (
		errs          []internal.LogRecord = make([]internal.LogRecord, 0)
		streamsIssues []Batcher            = make([]Batcher, 0)
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
	if len(r.streamsToDelete) > 0 {
		for siteCode, _ := range r.streamsToDelete {
			resStreamIssues, resErrLog := r.executeDeleteBatchedStreamsByStreamKey(siteCode)
			errs = append(errs, resErrLog...)
			streamsIssues = append(streamsIssues, resStreamIssues...)
		}
	}
	return streamsIssues, errs
}
