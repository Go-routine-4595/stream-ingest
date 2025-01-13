package cosmos

import (
	"context"
	"encoding/json"
	"errors"
	"githb.com/Go-routine-4595/stream-ingest/domain/stream"
	"githb.com/Go-routine-4595/stream-ingest/internal"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

const (
	OperationCreate = "create"
	OperationUpdate = "update"
)

type BatchProcessing struct {
	streams     []stream.Stream
	streamsByte [][]byte
	streamId    []string
	size        int
}

// CreatBatchedStreamsByStreamKey processes a single stream for batched creation by grouping and batching based on size limits.
// It returns unsent streams and errors if any exceed size limits or if marshaling fails.
func (r *Repository) CreatBatchedStreamsByStreamKey(streamItem stream.Stream) ([]stream.Stream, []internal.LogRecord) {

	streamByte, err := json.Marshal(streamItem)
	if err != nil {
		return []stream.Stream{streamItem}, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "failed to marshal item"}}
	}
	item := r.streamsToCreate[streamItem.SiteCode]
	item.streams = append(item.streams, streamItem)
	item.streamId = append(item.streamId, streamItem.ID)
	item.streamsByte = append(item.streamsByte, streamByte)
	item.size += len(streamByte)
	r.streamsToCreate[streamItem.SiteCode] = item
	if item.size > 3000000 {
		return r.executeCreateBatchedStreamsByStreamKey(streamItem.SiteCode)
	}
	return nil, nil
}

// executeCreateBatchedStreamsByStreamKey executes a transactional batch operation to create batched streams for a given siteCode.
// Returns streams with issues and errors if any part of the batch creation fails.
func (r *Repository) executeCreateBatchedStreamsByStreamKey(siteCode string) ([]stream.Stream, []internal.LogRecord) {

	pk := azcosmos.NewPartitionKeyString(siteCode)
	batchDB := r.Container.NewTransactionalBatch(pk)
	for _, item := range r.streamsToCreate[siteCode].streamsByte {
		batchDB.CreateItem(item, nil)
	}

	return r.executeBatch(batchDB, siteCode, OperationCreate)
}

// UpdateBatchedStreamsByStreamKey processes a single stream for batched updates by grouping and batching based on size limits.
// It adds streams to an update queue and returns unsent streams and errors if marshaling fails or size limits are exceeded.
func (r *Repository) UpdateBatchedStreamsByStreamKey(streamItem stream.Stream) ([]stream.Stream, []internal.LogRecord) {

	streamByte, err := json.Marshal(streamItem)
	if err != nil {
		return []stream.Stream{streamItem}, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "failed to marshal item"}}
	}
	item := r.streamsToUpdate[streamItem.SiteCode]
	item.streams = append(item.streams, streamItem)
	item.streamId = append(item.streamId, streamItem.ID)
	item.streamsByte = append(item.streamsByte, streamByte)
	item.size += len(streamByte)
	r.streamsToUpdate[streamItem.SiteCode] = item
	if item.size > 3000000 {
		return r.executeUpdateBatchedStreamsByStreamKey(streamItem.SiteCode)
	}
	return nil, nil
}

// executeUpdateBatchedStreamsByStreamKey executes a batch update operation on streams for a specified siteCode.
// It processes streams grouped in the update queue and uses ReplaceItem for each stream in the batch.
// Returns streams with issues and associated errors if the operation fails partially or completely.
func (r *Repository) executeUpdateBatchedStreamsByStreamKey(siteCode string) ([]stream.Stream, []internal.LogRecord) {

	pk := azcosmos.NewPartitionKeyString(siteCode)
	batchDB := r.Container.NewTransactionalBatch(pk)
	for i, item := range r.streamsToUpdate[siteCode].streamsByte {
		batchDB.ReplaceItem(r.streamsToUpdate[siteCode].streams[i].ID, item, nil)
	}

	return r.executeBatch(batchDB, siteCode, OperationUpdate)
}

// executeBatch executes a transactional batch operation and returns streams with issues and corresponding errors if any occur.
func (r *Repository) executeBatch(batchDB azcosmos.TransactionalBatch, siteCode string, operation string) ([]stream.Stream, []internal.LogRecord) {
	var (
		errs          []internal.LogRecord = make([]internal.LogRecord, 0)
		streamsIssues []stream.Stream      = make([]stream.Stream, 0)
	)

	ctx := context.TODO()

	resp, err := r.Container.ExecuteTransactionalBatch(ctx, batchDB, nil)
	if err != nil {
		return nil, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "failed to execute batch"}}
	}
	for i, op := range resp.OperationResults {
		if op.StatusCode != int32(http.StatusOK) && op.StatusCode != int32(http.StatusCreated) {
			lerr := errors.Join(errors.New("failed to create item in repository CreatBatchedStreamsByStreamKey"), err)
			logErr := internal.LogRecord{Err: lerr, Msg: "failed to create item in repository CreatBatchedStreamsByStreamKey"}
			errs = append(errs, logErr)
			//TODO we should return the stream that had issue
			switch operation {
			case OperationCreate:
				streamsIssues = append(streamsIssues, r.streamsToCreate[siteCode].streams[i])
			case OperationUpdate:
				streamsIssues = append(streamsIssues, r.streamsToUpdate[siteCode].streams[i])
			}

		}
	}
	if len(errs) > 0 {
		return streamsIssues, errs
	}
	return nil, nil
}
