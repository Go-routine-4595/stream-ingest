package cosmos

import (
	"context"
	"encoding/json"
	"errors"
	"githb.com/Go-routine-4595/stream-ingest/domain/stream"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/rs/zerolog/log"
)

type Repository struct {
	Client    *azcosmos.Client
	Container *azcosmos.ContainerClient
}

func NewRespository() Repository {
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

	return Repository{
		Client:    client,
		Container: container,
	}
}

// GetStreamByStreamIdAndSiteCode retrieves a stream from the repository using the provided stream ID. Returns the stream or an error.
func (r Repository) GetStreamByStreamIdAndSiteCode(sensorId string, siteCode string) ([]stream.Stream, error) {
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

func (r Repository) UpdateStreamsByStreamKey(streams []stream.Stream) []error {
	var errs []error

	for _, streamEle := range streams {
		itemData, err := json.Marshal(streamEle)
		if err != nil {
			//log.Logger.Debug().Msgf("Failed to marshal item: %v", err)
			lerr := errors.Join(errors.New("failed to marshal item in repository UpdateStreamsByStreamKey"), err)
			errs = append(errs, lerr)
		}
		// create a context
		ctx := context.TODO()

		pk := azcosmos.NewPartitionKeyString(streamEle.SiteCode)
		itemResponse, err := r.Container.ReplaceItem(ctx, pk, streamEle.ID, itemData, nil)
		_ = itemResponse
		if err != nil {
			//log.Logger.Debug().Msgf("Failed to insert item: %v", err)
			lerr := errors.Join(errors.New("failed to insert item in repository UpdateStreamsByStreamKey"), err)
			errs = append(errs, lerr)
		}
		//log.Logger.Debug().Msgf("Item created with ETag: %v\n", itemResponse.ETag)
	}

	return errs
}

func (r Repository) CreatStreamsByStreamKey(streams []stream.Stream) []error {
	var errs []error

	for _, streamEle := range streams {
		itemData, err := json.Marshal(streamEle)
		if err != nil {
			//log.Logger.Debug().Msgf("Failed to marshal item: %v", err)
			lerr := errors.Join(errors.New("failed to marshal item in repository UpdateStreamsByStreamKey"), err)
			errs = append(errs, lerr)
		}
		// create a context
		ctx := context.TODO()

		pk := azcosmos.NewPartitionKeyString(streamEle.SiteCode)
		itemResponse, err := r.Container.CreateItem(ctx, pk, itemData, nil)
		_ = itemResponse
		if err != nil {
			//log.Logger.Debug().Msgf("Failed to insert item: %v", err)
			lerr := errors.Join(errors.New("failed to insert item in repository UpdateStreamsByStreamKey"), err)
			errs = append(errs, lerr)
		}
		//log.Logger.Debug().Msgf("Item created with ETag: %v\n", itemResponse.ETag)
	}

	return errs
}

func (r Repository) Close() {

}
