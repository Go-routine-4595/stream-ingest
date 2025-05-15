// Package base
// -----------------------------------------------------------------------------
// File: Base.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//	            base is the common component between Stream and Constant, it
//	            represents a stream FCTS canonical data model in CosmoDB
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
package base

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"math"
	"time"

	"fmi/stream-ingest/domain/definition"
)

const (
	MaxAllowedUnknownTag = 10
)

type constraintType struct {
	tagType string
	repeats bool
}

// ConstraintTags is a map associating constraint names with their types as strings, used for identifying constraints.
var constraintTags map[string]constraintType = map[string]constraintType{
	definition.CollarElevation:      {tagType: definition.ConstraintAny, repeats: false},
	definition.EquipmentClass:       {tagType: definition.ConstraintAny, repeats: false},
	definition.EquipmentComponent:   {tagType: definition.ConstraintAny, repeats: false},
	definition.EquipmentMeasurement: {tagType: definition.ConstraintAny, repeats: false},
	definition.EquipmentName:        {tagType: definition.ConstraintString, repeats: false},
	definition.EquipmentSubUnit:     {tagType: definition.ConstraintAny, repeats: false},
	definition.EquipmentType:        {tagType: definition.ConstraintString, repeats: false},
	definition.EquipmentUnit:        {tagType: definition.ConstraintAny, repeats: false},
	definition.GaugeFactor:          {tagType: definition.ConstrainDecimalPoint, repeats: false},
	definition.GPSLatitude:          {tagType: definition.ConstrainDecimalPoint, repeats: false},
	definition.GPSLongitude:         {tagType: definition.ConstrainDecimalPoint, repeats: false},
	definition.Interpolation:        {tagType: definition.ConstrainDecimalPoint, repeats: false},
	definition.OpStatsLoader:        {tagType: definition.ConstraintString, repeats: false},
	definition.SAPEquipmentID:       {tagType: definition.ConstraintSAPID, repeats: false},
	definition.SAPMeasurementID:     {tagType: definition.ConstraintNumber, repeats: false},
	definition.SAPMeasurementType:   {tagType: definition.ConstraintString, repeats: false},
	definition.SAPUOM:               {tagType: definition.ConstraintString, repeats: false},
	definition.Scaling:              {tagType: definition.ConstraintNumber, repeats: false},
	definition.SensorElevation:      {tagType: definition.ConstrainDecimalPoint, repeats: false},
	definition.SIMS:                 {tagType: definition.ConstraintString, repeats: false},
	definition.System:               {tagType: definition.ConstraintString, repeats: false},
	definition.UDE:                  {tagType: definition.ConstraintString, repeats: false},
	definition.Workflow:             {tagType: definition.ConstraintString, repeats: false},
	definition.ZeroReading:          {tagType: definition.ConstraintNumber, repeats: false},
	definition.SiteShortCode:        {tagType: definition.ConstraintString, repeats: false},
	definition.OEM:                  {tagType: definition.ConstraintString, repeats: false},
	definition.Severity:             {tagType: definition.ConstraintString, repeats: false},
	definition.Delay:                {tagType: definition.ConstraintNumber, repeats: false},
}

type Validatable interface {
	Validate() error // Renamed from Validate
}

type Comparable[T any] interface {
	CompareTo(other T) bool // Renamed from CompareTo, added type parameter for clarity
}

type Identifiable interface {
	GetInternalID() string // Improved name for clarity
	GetID() string
}

type Updatable[T any] interface {
	UpdateWith(other T, user string) // Renamed from UpdateWith, added type parameter for clarity
}

type NumericalProcessor interface {
	ProcessNumericalValue()
}

type RowConvertible[T any] interface {
	ToRow() []string
}

type Deletable interface {
	Delete(uer string)
}

type StatusProvider interface {
	GetStatus() string
}

// Base represents the structure of the stream item.
type Base struct {
	ID         string  `json:"id"`
	Index      int     `json:"index"`
	SiteCode   string  `json:"siteCode"`
	Process    string  `json:"process"`
	UOM        string  `json:"uom"`
	Precision  float32 `json:"precision"`
	MinValue   float32 `json:"minValue"`
	MaxValue   float32 `json:"maxValue"`
	LoLo       float32 `json:"loLo"`
	Lo         float32 `json:"lo"`
	Hi         float32 `json:"hi"`
	HiHi       float32 `json:"hiHi"`
	Step       bool    `json:"step"`
	Tags       []Tag   `json:"tags"` // To be filled later
	Status     string  `json:"status"`
	Version    int     `json:"version"`
	CreatedBy  string  `json:"createdBy"`
	UpdatedBy  string  `json:"updatedBy"`
	CreatedUtc string  `json:"createdUtc"`
	UpdatedUtc string  `json:"updatedUtc"`
}

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// NewBase creates and returns a new Stream with default values.
func NewBase(user string) *Base {
	return &Base{
		ID:         uuid.NewString(),
		Index:      1,
		SiteCode:   "",
		Process:    "",
		UOM:        "",
		Precision:  float32(math.NaN()),
		MinValue:   float32(math.NaN()),
		MaxValue:   float32(math.NaN()),
		LoLo:       float32(math.NaN()),
		Lo:         float32(math.NaN()),
		Hi:         float32(math.NaN()),
		HiHi:       float32(math.NaN()),
		Step:       true,
		Tags:       make([]Tag, 0), // To be filled later
		Status:     "active",
		Version:    1,
		CreatedBy:  user,
		UpdatedBy:  user,
		CreatedUtc: FormatUtcTimestamp(time.Now()),
		UpdatedUtc: FormatUtcTimestamp(time.Now()),
	}
}

// SetCreationBy initializes CreatedBy, UpdatedBy, CreatedUtc, and UpdatedUtc fields using the provided user and current UTC time.
func (s *Base) SetCreationBy(user string) {
	s.CreatedBy = user
	s.UpdatedBy = user
	s.CreatedUtc = FormatUtcTimestamp(time.Now())
	s.UpdatedUtc = FormatUtcTimestamp(time.Now())
}

// SetUpdateBy updates the UpdatedBy and UpdatedUtc fields of the Stream object with the provided user and current UTC time.
func (s *Base) SetUpdateBy(user string) {
	s.UpdatedBy = user
	s.UpdatedUtc = FormatUtcTimestamp(time.Now())
}

// ToRowWithTags flattens the Tags field of the Stream into a slice of string values and returns it as a row.
// we guarantee that the result []string will always have the tag in the same order. If unknown tags are in the
// in the stream it works as well up to maxAllowedUnknownTag
func (s *Base) ToRowWithTags() []string {
	// we assume we can have up to 10 unknown tags...
	var (
		row        []string = make([]string, len(definition.TagsMap)+MaxAllowedUnknownTag)
		index      uint
		ok         bool
		unknownTag map[string]uint = make(map[string]uint)
	)

	for _, v := range s.Tags {
		if index, ok = definition.TagsMapIndex[v.Name]; !ok {
			if index, ok = unknownTag[v.Name]; !ok {
				if len(unknownTag) >= MaxAllowedUnknownTag {
					log.Fatal().Msgf("Too many unknown tags in stream %s", s.ID)
					continue
				}
				hash := hashStringToInt(v.Name)
				unknownTag[v.Name] = hash%MaxAllowedUnknownTag + uint(len(definition.TagsMap))
				index = unknownTag[v.Name]
			}
		}
		row[index] = v.Value
	}

	return row
}

// TagsHeader generates a slice of unique tag keys from the Stream's Tags field and returns it as a header row.
func (s *Base) TagsHeader() []string {
	var header []string
	for _, tag := range s.Tags {
		header = append(header, tag.Value)
	}
	return header
}

// ProcessNumericalValue checks numeric fields for NaN values and assigns default values if needed.
// Returns the updated Stream object with sanitized numeric fields.
func (s *Base) ProcessNumericalValue() {
	if math.IsNaN(float64(s.MinValue)) {
		s.MinValue = 0
	}
	if math.IsNaN(float64(s.MaxValue)) {
		s.MaxValue = 0
	}
	if math.IsNaN(float64(s.LoLo)) {
		s.LoLo = 0
	}
	if math.IsNaN(float64(s.Lo)) {
		s.Lo = 0
	}
	if math.IsNaN(float64(s.Hi)) {
		s.Hi = 0
	}
	if math.IsNaN(float64(s.HiHi)) {
		s.HiHi = 0
	}
	if math.IsNaN(float64(s.Precision)) {
		s.Precision = 2
	}
}

// AddTag adds a new tag with the provided name and value to the Stream's Tags field.
func (s *Base) AddTag(name string, value string) error {
	var err error

	// check any constraint of the tags
	err = IsTagConstraintTypeValid(name, value)
	if err != nil {
		return errors.Join(errors.New(definition.ErrBaseInvalidTagConstraint), err)
	}
	tag := Tag{
		Name:  name,
		Value: value,
	}

	// check if we don't have duplicates
	tmpTagList := make([]Tag, len(s.Tags)+1)
	tmpTagList = append(tmpTagList, s.Tags...)
	tmpTagList = append(tmpTagList, tag)
	err = CheckForDuplicatedTags(tmpTagList)
	if err != nil {
		return err
	}

	s.Tags = append(s.Tags, tag)
	return nil
}

// Validate checks the Stream.
func (s *Base) Validate() error {
	var err error

	err = CheckForDuplicatedTags(s.Tags)
	if err != nil {
		return err
	}
	for _, tag := range s.Tags {
		err = IsTagConstraintTypeValid(tag.Name, tag.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Base) Mapper(header string, value string) error {
	var err error

	switch header {

	case definition.CsvSiteCode:
		if !IsSiteCodeValid(value) {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidSiteCode, value)
		}
		s.SiteCode = value
	case definition.CsvProcess:
		d := []byte(value)
		_ = d
		if IsProcess(value) {
			s.Process = value
		} else {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidProcess, value)
		}
	case definition.CsvMinValue:
		s.MinValue, err = GetFloatValueFrom(value)
		if err != nil {
			return errors.Join(errors.New(definition.ErrBaseInvalidMinValue), err)
		}
	case definition.CsvMaxValue:
		d := []byte(value)
		_ = d
		s.MaxValue, err = GetFloatValueFrom(value)
		if err != nil {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidMaxValue, value)
		}
	case definition.CsvUom:
		s.UOM = value
	case definition.CsvLoLo:
		s.LoLo, err = GetFloatValueFrom(value)
		if err != nil {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidLoLo, value)
		}
	case definition.CsvLo:
		s.Lo, err = GetFloatValueFrom(value)
		if err != nil {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidLo, value)
		}
	case definition.CsvHi:
		s.Hi, err = GetFloatValueFrom(value)
		if err != nil {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidHi, value)
		}
	case definition.CsvHiHi:
		s.HiHi, err = GetFloatValueFrom(value)
		if err != nil {
			return fmt.Errorf("%s parsing:\"%s\" ", definition.ErrBaseInvalidHiHi, value)
		}
	default:
		// this check id done in AddTag
		//if ok, err := base.IsTagConstraintTypeValid(header, value); !ok {
		//	return errors.Join(errors.New(definition.ErrBaseInvalidTagConstraint), err)
		//}
		if value == "" {
			break
		}
		err = s.AddTag(header, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// CompareTo compares two Stream objects and returns true if they are identical, otherwise false.
func (s *Base) CompareTo(c2 Base) bool {
	// CompareTo field by field
	// excluding index
	return s.SiteCode == c2.SiteCode &&
		s.Process == c2.Process &&
		CompareString(s.UOM, c2.UOM) &&
		CompareNumValue(s.Precision, c2.Precision) &&
		CompareNumValue(s.MinValue, c2.MinValue) &&
		CompareNumValue(s.MaxValue, c2.MaxValue) &&
		CompareNumValue(s.LoLo, c2.LoLo) &&
		CompareNumValue(s.Lo, c2.Lo) &&
		CompareNumValue(s.Hi, c2.Hi) &&
		CompareNumValue(s.HiHi, c2.HiHi) &&
		CompareTags(s.Tags, c2.Tags)
}

// UpdateWith updates the fields of base1 with the fields of base2.
// and UpdateTag set the modifying by/and date
func (s *Base) UpdateWith(c2 Base, user string) {
	s.Process = CopyString(s.Process, c2.Process)
	s.UOM = CopyString(s.UOM, c2.UOM)
	s.Precision = CopyNumValue(s.Precision, c2.Precision)
	s.MinValue = CopyNumValue(s.MinValue, c2.MinValue)
	s.MaxValue = CopyNumValue(s.MaxValue, c2.MaxValue)
	s.LoLo = CopyNumValue(s.LoLo, c2.LoLo)
	s.Lo = CopyNumValue(s.Lo, c2.Lo)
	s.Hi = CopyNumValue(s.Hi, c2.Hi)
	s.HiHi = CopyNumValue(s.HiHi, c2.HiHi)
	s.Tags, _ = UpdateTags(s.Tags, c2.Tags)
	s.SetUpdateBy(user)
}

// GetInternalID returns the internal unique identifier (ID) of the Base object as a string.
// implement the cosmos.Batcher interface
func (s *Base) GetInternalID() string {
	return s.ID
}

func (s *Base) Delete(user string) {
	s.SetUpdateBy(user)
	s.Status = "deleted"
}

func (s *Base) GetStatus() string {
	return s.Status
}

// DeepCopy creates a deep copy of the Base structure.
// It returns a new Base instance with all fields copied.
func (b *Base) DeepCopy() *Base {
	if b == nil {
		return nil
	}

	// Create a new Base structure
	dcopy := &Base{
		ID:         b.ID,
		Index:      b.Index,
		SiteCode:   b.SiteCode,
		Process:    b.Process,
		UOM:        b.UOM,
		Precision:  b.Precision,
		MinValue:   b.MinValue,
		MaxValue:   b.MaxValue,
		LoLo:       b.LoLo,
		Lo:         b.Lo,
		Hi:         b.Hi,
		HiHi:       b.HiHi,
		Step:       b.Step,
		Status:     b.Status,
		Version:    b.Version,
		CreatedBy:  b.CreatedBy,
		UpdatedBy:  b.UpdatedBy,
		CreatedUtc: b.CreatedUtc,
		UpdatedUtc: b.UpdatedUtc,
	}

	// Deep copy the Tags slice
	if b.Tags != nil {
		dcopy.Tags = make([]Tag, len(b.Tags))
		for i, tag := range b.Tags {
			// Copy each Tag
			dcopy.Tags[i] = Tag{
				Name:  tag.Name,
				Value: tag.Value,
			}
		}
	}

	return dcopy
}
