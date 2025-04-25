package model

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmi/stream-ingest/domain/base"
	"fmi/stream-ingest/domain/constant"
	"fmi/stream-ingest/domain/stream"
	"fmi/stream-ingest/repository/cosmos"
	"fmi/stream-ingest/repository/dataprocessor"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
)

const (
	// ElementTypeStream represents a stream element
	ElementTypeStream = "stream"
	// ElementTypeConstant represents a constant element
	ElementTypeConstant = "constant"
)

// RegistryInterface combines multiple interfaces for data mapping, batching, identification, comparison, updating, validation, and numerical processing.
type RegistryInterface interface {
	dataprocessor.Mapper
	cosmos.Batcher
	base.Identifiable
	base.Comparable[any]
	base.Updatable[any]
	base.Validatable
	base.NumericalProcessor
	base.RowConvertible[any]
	base.Deletable
	base.StatusProvider
}

type RepositoryInterface interface {
	GetConstantByNameAndSiteCode(sensorId string, siteCode string) ([]constant.Constant, error)
	GetStreamByStreamIdAndSiteCode(sensorId string, siteCode string) ([]stream.Stream, error)
	GetDataBatchBySideCode(siteCode string, ch chan<- []byte, dataT string, ctx context.Context)
}

type Registry struct {
	elementType string
	headers     []string
}

// Registry type for dumping const and streams
func NewRegistryFromType(elementType string) (*Registry, error) {
	switch elementType {
	case ElementTypeStream:
		streamHeaders := stream.GetExpectedHeader()
		streamHeaders = append(streamHeaders, stream.GetExpectedTags()...)
		return &Registry{
			elementType: elementType,
			headers:     streamHeaders,
		}, nil
	case ElementTypeConstant:
		constantHeaders := constant.GetExpectedHeader()
		constantHeaders = append(constantHeaders, constant.GetExpectedTags()...)
		return &Registry{
			elementType: elementType,
			headers:     constantHeaders,
		}, nil
	default:
		return nil, fmt.Errorf("invalid element type: %q", elementType)
	}
}

func (r *Registry) GetDataBatchBySideCode(repo RepositoryInterface, siteCode string, ch chan<- RegistryInterface, ctx context.Context) {
	// Channel to receive raw JSON data
	dataChannel := make(chan []byte, 100)

	// Request data from repository
	repo.GetDataBatchBySideCode(siteCode, dataChannel, r.elementType, ctx)

	go func() {
		defer close(ch)

		switch r.elementType {
		case ElementTypeStream:
			streamEl := new(stream.Stream)
			for item := range dataChannel {
				if err := json.Unmarshal(item, streamEl); err != nil {
					log.Error().Err(err).Msg("failed to unmarshal stream")
					return
				}
				ch <- streamEl.DeepCopy()
			}

		case ElementTypeConstant:
			constEl := new(constant.Constant)
			for item := range dataChannel {
				if err := json.Unmarshal(item, constEl); err != nil {
					log.Error().Err(err).Msg("failed to unmarshal constant")
					return
				}
				ch <- constEl.DeepCopy()
			}
		}
	}()

}

// Registry type for importing from CSV file
func NewRegistry(fromFile string) (*Registry, error) {
	elementType, headers, err := getExpectedHeaderType(fromFile)
	if err != nil {
		return nil, err
	}
	return &Registry{
		elementType: elementType,
		headers:     headers,
	}, nil
}

func (r *Registry) NewElement(user string) RegistryInterface {
	switch r.elementType {
	case ElementTypeStream:
		return stream.NewStream(user)
	case ElementTypeConstant:
		return constant.NewConstant(user)
	default:
		return nil
	}
}

func (r *Registry) GetHeaders() []string {
	return r.headers
}

func (r *Registry) GetElementFromRepo(repo RepositoryInterface, id string, siteCode string) ([]RegistryInterface, error) {
	var elements []RegistryInterface

	switch r.elementType {
	case ElementTypeStream:
		streams, err := repo.GetStreamByStreamIdAndSiteCode(id, siteCode)
		if err != nil {
			return nil, fmt.Errorf("failed to get streams: %w", err)
		}
		for i := range streams {
			elements = append(elements, &streams[i])
		}

	case ElementTypeConstant:
		constants, err := repo.GetConstantByNameAndSiteCode(id, siteCode)
		if err != nil {
			return nil, fmt.Errorf("failed to get constants: %w", err)
		}
		for i := range constants {
			elements = append(elements, &constants[i])
		}

	default:
		return nil, fmt.Errorf("invalid element type: %q", r.elementType)
	}

	return elements, nil
}

func getExpectedHeaderType(file string) (string, []string, error) {
	var (
		ok         bool
		missingKey string
	)

	// Open the file
	f, err := os.Open(file)
	if err != nil {
		return "", nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Read headers
	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		return "", nil, fmt.Errorf("failed to read headers: %w", err)
	}

	// Check if headers match stream type
	if ok, missingKey = isSameHeaders(stream.GetExpectedHeader(), headers); ok {
		return string(ElementTypeStream), stream.GetExpectedHeader(), nil
	}

	// Check if headers match constant type
	if ok, missingKey = isSameHeaders(constant.GetExpectedHeader(), headers); ok {
		return string(ElementTypeConstant), constant.GetExpectedHeader(), nil
	}

	return "", nil, fmt.Errorf("invalid headers in file - missing required header: %s", missingKey)

}

func isSameHeaders(reference []string, header []string) (bool, string) {
	var (
		headereMap map[string]bool
	)

	headereMap = make(map[string]bool)
	for i, _ := range header {
		// Remove BOM (0xEF 0xBB 0xBF if any, I guess MS excel UTF-8 stuff)
		if len(header[i]) >= 3 && header[i][0] == 0xEF && header[i][1] == 0xBB && header[i][2] == 0xBF {
			header[i] = header[i][3:] // Remove BOM
		}
		headereMap[header[i]] = true
	}

	for _, key := range reference {
		if !headereMap[key] {
			return false, key
		}
	}
	return true, ""
}
