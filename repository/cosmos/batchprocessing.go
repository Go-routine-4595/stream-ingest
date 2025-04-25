// Package cosmos
// -----------------------------------------------------------------------------
// File: batchprocessing.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            Batch processing function
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
	"errors"
	"fmi/stream-ingest/internal"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

const (
	OperationCreate = "create"
	OperationUpdate = "update"
	OperationDelete = "delete"
	batchSize       = 200000
	maxOperations   = 90
	maxItemSize     = 2000000
)

type Batcher interface {
	GetInternalID() string
	GetSiteCode() string
	ToRow() []string
}

type BatchProcessing struct {
	streams     []Batcher
	streamsByte [][]byte
	streamId    []string
	size        int
	operation   int
}

// CreatBatchedStreamsByStreamKey processes a single stream for batched creation by grouping and batching based on size limits.
// It returns unsent streams and errors if any exceed size limits or if marshaling fails.
func (r *Repository) CreatBatchedStreamsByStreamKey(batchItem Batcher) ([]Batcher, []internal.LogRecord) {

	streamByte, err := json.Marshal(batchItem)
	if err != nil {
		return []Batcher{batchItem}, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "failed to marshal item"}}
	}
	if len(streamByte) > maxItemSize {
		return []Batcher{batchItem}, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "item to large to be sent in batch, size: " + fmt.Sprintf("%d", len(streamByte)) + " max: " + fmt.Sprintf("%d", maxItemSize)}}
	}

	item := r.streamsToCreate[batchItem.GetSiteCode()]
	item.streams = append(item.streams, batchItem)
	item.streamId = append(item.streamId, batchItem.GetInternalID())
	item.streamsByte = append(item.streamsByte, streamByte)
	item.size += len(streamByte)
	item.operation++
	r.streamsToCreate[batchItem.GetSiteCode()] = item
	if item.size > batchSize || item.operation > maxOperations {
		return r.executeCreateBatchedStreamsByStreamKey(batchItem.GetSiteCode())
	}

	return nil, nil
}

// executeCreateBatchedStreamsByStreamKey executes a transactional batch operation to create batched streams for a given siteCode.
// Returns streams with issues and errors if any part of the batch creation fails.
func (r *Repository) executeCreateBatchedStreamsByStreamKey(siteCode string) ([]Batcher, []internal.LogRecord) {

	pk := azcosmos.NewPartitionKeyString(siteCode)
	batchDB := r.Container.NewTransactionalBatch(pk)
	for _, item := range r.streamsToCreate[siteCode].streamsByte {
		batchDB.CreateItem(item, nil)
	}
	errorStreams, errorLogs := r.executeBatch(batchDB, siteCode, OperationUpdate)
	// empty the batch
	batchItem := r.streamsToCreate[siteCode]
	batchItem.size = 0
	batchItem.operation = 0
	batchItem.streams = batchItem.streams[:0]
	batchItem.streamId = batchItem.streamId[:0]
	batchItem.streamsByte = batchItem.streamsByte[:0]
	r.streamsToCreate[siteCode] = batchItem

	return errorStreams, errorLogs
}

// DeleteBatchedStreamsByStreamKey processes a single stream for batched creation by grouping and batching based on size limits.
// It returns unsent streams and errors if any exceed size limits or if marshaling fails.
func (r *Repository) DeleteBatchedStreamsByStreamKey(batchItem Batcher) ([]Batcher, []internal.LogRecord) {

	item := r.streamsToDelete[batchItem.GetSiteCode()]
	item.streams = append(item.streams, batchItem)
	item.streamId = append(item.streamId, batchItem.GetInternalID())
	item.operation++
	r.streamsToDelete[batchItem.GetSiteCode()] = item
	if item.size > batchSize || item.operation > maxOperations {
		return r.executeDeleteBatchedStreamsByStreamKey(batchItem.GetSiteCode())
	}

	return nil, nil
}

// executeDeleteBatchedStreamsByStreamKey executes a transactional batch operation to create batched streams for a given siteCode.
// Returns streams with issues and errors if any part of the batch creation fails.
func (r *Repository) executeDeleteBatchedStreamsByStreamKey(siteCode string) ([]Batcher, []internal.LogRecord) {

	pk := azcosmos.NewPartitionKeyString(siteCode)
	batchDB := r.Container.NewTransactionalBatch(pk)
	for _, item := range r.streamsToDelete[siteCode].streamId {
		batchDB.DeleteItem(item, nil)
	}
	errorStreams, errorLogs := r.executeBatch(batchDB, siteCode, OperationDelete)
	time.Sleep(2 * time.Second)
	// empty the batch
	batchItem := r.streamsToCreate[siteCode]
	batchItem.size = 0
	batchItem.operation = 0
	batchItem.streams = batchItem.streams[:0]
	batchItem.streamId = batchItem.streamId[:0]
	batchItem.streamsByte = batchItem.streamsByte[:0]
	r.streamsToCreate[siteCode] = batchItem

	return errorStreams, errorLogs
}

// UpdateBatchedStreamsByStreamKey processes a single stream for batched updates by grouping and batching based on size limits.
// It adds streams to an update queue and returns unsent streams and errors if marshaling fails or size limits are exceeded.
func (r *Repository) UpdateBatchedStreamsByStreamKey(batchItem Batcher) ([]Batcher, []internal.LogRecord) {

	streamByte, err := json.Marshal(batchItem)
	if err != nil {
		return []Batcher{batchItem}, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "failed to marshal item"}}
	}
	item := r.streamsToUpdate[batchItem.GetSiteCode()]
	item.streams = append(item.streams, batchItem)
	item.streamId = append(item.streamId, batchItem.GetInternalID())
	item.streamsByte = append(item.streamsByte, streamByte)
	item.size += len(streamByte)
	item.operation++
	r.streamsToUpdate[batchItem.GetSiteCode()] = item
	if item.size > batchSize || item.operation > maxOperations {
		return r.executeUpdateBatchedStreamsByStreamKey(batchItem.GetSiteCode())
	}
	return nil, nil
}

// executeUpdateBatchedStreamsByStreamKey executes a batch update operation on streams for a specified siteCode.
// It processes streams grouped in the update queue and uses ReplaceItem for each stream in the batch.
// Returns streams with issues and associated errors if the operation fails partially or completely.
func (r *Repository) executeUpdateBatchedStreamsByStreamKey(siteCode string) ([]Batcher, []internal.LogRecord) {

	pk := azcosmos.NewPartitionKeyString(siteCode)
	batchDB := r.Container.NewTransactionalBatch(pk)
	for i, item := range r.streamsToUpdate[siteCode].streamsByte {
		batchDB.ReplaceItem(r.streamsToUpdate[siteCode].streams[i].GetInternalID(), item, nil)
	}

	errorStreams, errorLogs := r.executeBatch(batchDB, siteCode, OperationUpdate)
	// empty the batch
	batchItem := r.streamsToUpdate[siteCode]
	batchItem.size = 0
	batchItem.operation = 0
	batchItem.streams = batchItem.streams[:0]
	batchItem.streamId = batchItem.streamId[:0]
	batchItem.streamsByte = batchItem.streamsByte[:0]

	r.streamsToUpdate[siteCode] = batchItem

	return errorStreams, errorLogs
}

// executeBatch executes a transactional batch operation and returns streams with issues and corresponding errors if any occur.
func (r *Repository) executeBatch(batchDB azcosmos.TransactionalBatch, siteCode string, operation string) ([]Batcher, []internal.LogRecord) {
	var (
		errs          []internal.LogRecord = make([]internal.LogRecord, 0)
		streamsIssues []Batcher            = make([]Batcher, 0)
	)

	ctx := context.TODO()

	resp, err := r.Container.ExecuteTransactionalBatch(ctx, batchDB, nil)
	if err != nil {
		return nil, []internal.LogRecord{internal.LogRecord{Err: err, Msg: "failed to execute batch"}}
	}
	for i, op := range resp.OperationResults {
		if op.StatusCode != int32(http.StatusOK) && op.StatusCode != int32(http.StatusCreated) && op.StatusCode != int32(http.StatusNoContent) {
			lerr := errors.Join(fmt.Errorf("failed to %s item in repository executeBatch code %d ", operation, op.StatusCode), err)
			respBody, _ := readResponse(resp.RawResponse.Body)
			logErr := internal.LogRecord{Err: lerr, Msg: fmt.Sprintf("failed to %s item in repository executeBatch code %d  RawResponse body: %s", operation, op.StatusCode, string(respBody))}
			errs = append(errs, logErr)

			switch operation {
			case OperationCreate:
				if len(r.streamsToCreate[siteCode].streams) > i {
					streamsIssues = append(streamsIssues, r.streamsToCreate[siteCode].streams[i])
				}
			case OperationUpdate:
				if len(r.streamsToUpdate[siteCode].streams) > i {
					streamsIssues = append(streamsIssues, r.streamsToUpdate[siteCode].streams[i])
				}
			case OperationDelete:
				if len(r.streamsToDelete[siteCode].streams) > i {
					streamsIssues = append(streamsIssues, r.streamsToDelete[siteCode].streams[i])
				}
			}

		}
	}
	if len(errs) > 0 {
		return streamsIssues, errs
	}
	return nil, nil
}

func readResponse(resp io.ReadCloser) ([]byte, error) {
	defer resp.Close()
	b, err := io.ReadAll(resp)
	if err != nil {
		return nil, err
	}
	return b, nil
}
