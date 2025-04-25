// Package stream
// -----------------------------------------------------------------------------
// File: stream.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            stream is the main component, it represents a stream FCTS canonical
//	            data model in CosmoDB
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
package stream

import (
	"errors"
	"fmi/stream-ingest/domain/base"
	"fmi/stream-ingest/domain/definition"
	"math"
)

// ExpectedHeaders defines the list of strings representing the expected header names in a data processing context.
var expectedHeaders = []string{
	definition.CsvSiteCode,
	definition.CsvSensorId,
	definition.CsvStreamName,
	definition.CsvProcess,
	definition.CsvScaleFactor,
	definition.CsvMinValue,
	definition.CsvMaxValue,
	definition.CsvLoLo,
	definition.CsvLo,
	definition.CsvHi,
	definition.CsvHiHi,
	definition.CsvUom,
}

// Stream represents the structure of the stream item.
type Stream struct {
	*base.Base
	RegistryType string  `json:"registryType"`
	StreamName   string  `json:"streamName"`
	SensorID     string  `json:"sensorId"`
	ScaleFactor  float32 `json:"scaleFactor"`
}

func (s *Stream) GetInternalId() string {
	return s.Base.GetInternalID()
}

// NewStream creates and returns a new Stream with default values.
func NewStream(user string) *Stream {
	return &Stream{
		Base:         base.NewBase(user),
		RegistryType: "stream",
		StreamName:   "",
		SensorID:     "",
		ScaleFactor:  float32(math.NaN()),
	}
}

// ToRow converts the Stream object into a slice of strings, representing its fields and flattened tags.
// can be used to store in a CSV file
func (s *Stream) ToRow() []string {
	var row []string

	row = append(row, s.SiteCode)
	row = append(row, s.SensorID)
	row = append(row, s.StreamName)
	row = append(row, s.Process)
	row = append(row, base.ToString(s.ScaleFactor))
	row = append(row, base.ToString(s.MinValue))
	row = append(row, base.ToString(s.MaxValue))
	row = append(row, base.ToString(s.LoLo))
	row = append(row, base.ToString(s.Lo))
	row = append(row, base.ToString(s.Hi))
	row = append(row, base.ToString(s.HiHi))
	row = append(row, s.UOM)
	row = append(row, s.ToRowWithTags()...)

	return row
}

func (s *Stream) Mapper(header string, value string) error {
	var err error

	switch header {
	case definition.CsvSensorId:
		s.SensorID = value
		return nil
	case definition.CsvStreamName:
		s.StreamName = value
		return nil
	case definition.CsvScaleFactor:
		s.ScaleFactor, err = base.GetFloatValueFrom(value)
		if err != nil {
			return errors.Join(errors.New(definition.ErrBaseInvalidScaleFactor), err)
		}
		return nil
	}
	return s.Base.Mapper(header, value)
}

// GetID retrieves the unique identifier of the Stream instance. Returns the ID as a string.
func (s *Stream) GetID() string {
	return s.SensorID
}

// GetSiteCode retrieves the SiteCode property of the Stream instance as a string.
// implement the cosmos.Batcher interface
func (s *Stream) GetSiteCode() string {
	return s.SiteCode
}

// ProcessNumericalValue checks if the ScaleFactor is NaN and resets it to 1 if true, then processes numeric values via Base.
func (s *Stream) ProcessNumericalValue() {
	if math.IsNaN(float64(s.ScaleFactor)) {
		s.ScaleFactor = 1
	}
	s.Base.ProcessNumericalValue()
}

// CompareTo compares two Stream objects and returns true if they are identical, otherwise false.
func (s *Stream) CompareTo(other any) bool {
	// CompareTo field by field
	// excluding index
	s2, ok := other.(*Stream)
	if !ok {
		return false
	}
	return s.Base.CompareTo(*s2.Base) &&
		s.SensorID == s2.SensorID &&
		s.StreamName == s2.StreamName &&
		s.ScaleFactor == s2.ScaleFactor
}

// UpdateWith updates the fields of constant1 with the fields of constant2.
// and UpdateTag set the modifying by/and date
func (s *Stream) UpdateWith(T any, user string) {
	s2, ok := T.(*Stream)
	if !ok {
		return
	}
	s.Base.UpdateWith(*s2.Base, user)
	s.SensorID = base.CopyString(s.SensorID, s2.SensorID)
	s.StreamName = base.CopyString(s.StreamName, s2.StreamName)
	s.ScaleFactor = base.CopyNumValue(s.ScaleFactor, s2.ScaleFactor)
	s.SetUpdateBy(user)
}

func (s *Stream) Delete(user string) {
	s.SetUpdateBy(user)
	s.Status = "deleted"
}

func (s *Stream) GetRegistryType() string {
	return s.RegistryType
}

func (s *Stream) GetStatus() string {
	return s.Status
}

// DeepCopy creates a deep copy of the Stream structure.
// It returns a new Stream instance with all fields copied.
func (s *Stream) DeepCopy() *Stream {
	if s == nil {
		return nil
	}

	// Create a new Stream
	copied := &Stream{
		// Deep copy the Base pointer (assuming base.Base has a DeepCopy method)
		// If it doesn't, we'll need to manually copy each field from the base
		Base:         s.Base.DeepCopy(),
		RegistryType: s.RegistryType,
		StreamName:   s.StreamName,
		SensorID:     s.SensorID,
		ScaleFactor:  s.ScaleFactor,
	}

	return copied
}
