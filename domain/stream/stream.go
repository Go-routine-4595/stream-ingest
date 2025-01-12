package stream

import (
	"fmt"
	"githb.com/Go-routine-4595/stream-ingest/model"
	"github.com/google/uuid"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Tags
const (
	EquipmentClass       = "EquipmentClass"
	EquipmentComponent   = "EquipmentComponent"
	EquipmentMeasurement = "EquipmentMeasurement"
	EquipmentName        = "EquipmentName"
	EquipmentType        = "EquipmentType"
	Interpolation        = "Interpolation"
	OpStatsLoader        = "OpStatsLoader"
	SAPEquipmentID       = "SAPEquipmentID"
	SAPMeasurementID     = "SAPMeasurementID"
	SAPMeasurementType   = "SAPMeasurementType"
	SAPUOM               = "SAPUOM"
	Scaling              = "Scaling"
	SIMS                 = "SIMS"
	UDE                  = "UDE"
	Workflow             = "Workflow"
	SiteShortCode        = "SiteShortCode"
)

// Process
const (
	CNCCrushConvey          = "CNC"
	ENVEnvironmental        = "ENV"
	GDEEquipmentManagement  = "GDE"
	FTCFMCTechnologyCenter  = "FTC"
	FNLFragmentationLoading = "FNL"
	General                 = "GEN"
	Haulage                 = "HAU"
	HM                      = "HM"
	Leaching                = "LEA"
	MN                      = "MN"
	MO                      = "MO"
	MIS                     = "MIS"
	REF                     = "REF"
	ROD                     = "ROD"
	SMLSmelting             = "SML"
	TCLWTCLW                = "TCLW"
	GMX                     = "GMX"
)

// IsProcess helper function
func IsProcess(s string) bool {
	return s == CNCCrushConvey ||
		s == ENVEnvironmental ||
		s == GDEEquipmentManagement ||
		s == FTCFMCTechnologyCenter ||
		s == FNLFragmentationLoading ||
		s == General ||
		s == Haulage ||
		s == HM ||
		s == Leaching ||
		s == MN ||
		s == MO ||
		s == MIS ||
		s == REF ||
		s == ROD ||
		s == SMLSmelting ||
		s == TCLWTCLW ||
		s == GMX

}

// IsTag helper function
func IsTag(s string) bool {
	return s == EquipmentClass ||
		s == EquipmentComponent ||
		s == EquipmentMeasurement ||
		s == EquipmentName ||
		s == EquipmentType ||
		s == Interpolation ||
		s == OpStatsLoader ||
		s == SAPEquipmentID ||
		s == SAPMeasurementID ||
		s == SAPMeasurementType ||
		s == SAPUOM ||
		s == Scaling ||
		s == SIMS ||
		s == UDE ||
		s == Workflow ||
		s == SiteShortCode
}

// Stream represents the structure of the stream item.
type Stream struct {
	ID           string        `json:"id"`
	RegistryType string        `json:"registryType"`
	Index        int           `json:"index"`
	SiteCode     string        `json:"siteCode"`
	Process      string        `json:"process"`
	StreamName   string        `json:"streamName"`
	SensorID     string        `json:"sensorId"`
	UOM          string        `json:"uom"`
	ScaleFactor  int           `json:"scaleFactor"`
	Precision    int           `json:"precision"`
	MinValue     int           `json:"minValue"`
	MaxValue     int           `json:"maxValue"`
	LoLo         int           `json:"loLo"`
	Lo           int           `json:"lo"`
	Hi           int           `json:"hi"`
	HiHi         int           `json:"hiHi"`
	Step         bool          `json:"step"`
	Tags         []interface{} `json:"tags"` // To be filled later
	Status       string        `json:"status"`
	Version      int           `json:"version"`
	CreatedBy    string        `json:"createdBy"`
	UpdatedBy    string        `json:"updatedBy"`
	CreatedUtc   string        `json:"createdUtc"`
	UpdatedUtc   string        `json:"updatedUtc"`
}

// NewStream creates and returns a new Stream with default values.
func NewStream() Stream {
	return Stream{
		ID:           uuid.NewString(),
		RegistryType: "stream",
		Index:        1,
		SiteCode:     "",
		Process:      "",
		StreamName:   "",
		SensorID:     "",
		UOM:          "",
		ScaleFactor:  0,
		Precision:    0,
		MinValue:     0,
		MaxValue:     0,
		LoLo:         0,
		Lo:           0,
		Hi:           0,
		HiHi:         0,
		Step:         true,
		Tags:         []interface{}{}, // To be filled later
		Status:       "active",
		Version:      1,
	}
}

// Helper function to format UTC time to ISO 8601 with microsecond precision ending in "Z"
func formatUtcTimestamp(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000000Z")
}

func (s Stream) SetCreationBy(user string) Stream {
	s.CreatedBy = user
	s.UpdatedBy = user
	s.CreatedUtc = formatUtcTimestamp(time.Now())
	s.UpdatedUtc = formatUtcTimestamp(time.Now())
	return s
}

func (s Stream) SetUpdateBy(user string) Stream {
	s.UpdatedBy = user
	s.UpdatedUtc = formatUtcTimestamp(time.Now())
	return s
}

// UpdateTags updates the Tags field by adding new tags that are not already present
func UpdateTags(stream1 *Stream, stream2 *Stream, user string) {
	existingTagsSet := make(map[model.Tag]bool)

	// Add existing tags to the set for quick lookup
	for _, tag := range stream1.Tags {
		var t model.Tag
		t.Value = tag.(map[string]interface{})["value"].(string)
		t.Name = tag.(map[string]interface{})["name"].(string)
		existingTagsSet[t] = true
	}

	// Add only the new tags that are not already in the set
	for _, tag := range stream2.Tags {
		if _, exists := existingTagsSet[tag.(model.Tag)]; !exists {
			var interfaceValue interface{} = tag
			stream1.Tags = append(stream1.Tags, interfaceValue)
		}
	}
	*stream1 = stream1.SetUpdateBy(user)

}

// UpdateStreamOld updates the fields of stream1 with the fields of stream2.
// Only non-zero or non-default values from stream2 will overwrite those in stream1.
func UpdateStreamOld(stream1 *Stream, stream2 *Stream, user string) {
	v1 := reflect.ValueOf(stream1).Elem()
	v2 := reflect.ValueOf(stream2).Elem()

	for i := 0; i < v1.NumField(); i++ {
		field1 := v1.Field(i)
		field2 := v2.Field(i)

		if field2.IsValid() && field2.Interface() != reflect.Zero(field2.Type()).Interface() {
			// Skip updating the CreatedUtc and CreatedBy field if required to preserve its originality
			if name := v1.Type().Field(i).Name; name == "CreatedUtc" {
				continue
			}
			if name := v1.Type().Field(i).Name; name == "CreatedBy" {
				continue
			}
			if name := v1.Type().Field(i).Name; name == "UpdatedUtc" {
				field1.Set(reflect.ValueOf(formatUtcTimestamp(time.Now())))
				continue
			}
			if name := v1.Type().Field(i).Name; name == "UpdateBy" {
				field1.Set(reflect.ValueOf(user))
				continue
			}
			if name := v1.Type().Field(i).Name; name == "ID" {
				continue
			}
			field1.Set(field2)
		}
	}
}

// UpdateStream updates the fields of stream1 with the fields of stream2.
// and UpdateTag set the modify by/and date
func UpdateStream(s1 *Stream, s2 *Stream, user string) {
	s1.Process = s2.Process
	s1.StreamName = s2.StreamName
	s1.UOM = s2.UOM
	s1.ScaleFactor = s2.ScaleFactor
	s1.Precision = s2.Precision
	s1.MinValue = s2.MinValue
	s1.MaxValue = s2.MaxValue
	s1.LoLo = s2.LoLo
	s1.Lo = s2.Lo
	s1.Hi = s2.Hi
	s1.HiHi = s2.HiHi
	UpdateTags(s1, s2, user)
}

// CompareStreams compares two Stream objects and returns true if they are identical, otherwise false.
func CompareStreams(s1 Stream, s2 Stream) bool {
	// Compare field by field
	return s1.ID == s2.ID &&
		s1.RegistryType == s2.RegistryType &&
		s1.Index == s2.Index &&
		s1.SiteCode == s2.SiteCode &&
		s1.Process == s2.Process &&
		s1.StreamName == s2.StreamName &&
		s1.SensorID == s2.SensorID &&
		s1.UOM == s2.UOM &&
		s1.ScaleFactor == s2.ScaleFactor &&
		s1.Precision == s2.Precision &&
		s1.MinValue == s2.MinValue &&
		s1.MaxValue == s2.MaxValue &&
		s1.LoLo == s2.LoLo &&
		s1.Lo == s2.Lo &&
		s1.Hi == s2.Hi &&
		s1.HiHi == s2.HiHi &&
		compareTags(s1.Tags, s2.Tags)
}

// compareTags is a helper function to compare two slices of interface{} representing tags.
func compareTags(tags1 []interface{}, tags2 []interface{}) bool {
	if len(tags1) != len(tags2) {
		return false
	}

	// Convert tags slices to maps for easy comparison
	tagMap1 := make(map[model.Tag]bool)
	tagMap2 := make(map[model.Tag]bool)

	for _, tag := range tags1 {
		if t, ok := tag.(model.Tag); ok {
			tagMap1[t] = true
		}
	}

	for _, tag := range tags2 {
		if t, ok := tag.(model.Tag); ok {
			tagMap2[t] = true
		}
	}

	// Compare tag maps
	if len(tagMap1) != len(tagMap2) {
		return false
	}

	for tag := range tagMap1 {
		if !tagMap2[tag] {
			return false
		}
	}

	return true
}

// ConvertStreamToItem converts a Stream structure to an Item structure.
func (s Stream) ConvertStreamToItem() model.Item {
	// Convert Tags from []interface{} to []Tag
	var tags []model.Tag
	for _, t := range s.Tags {
		//if tag, ok := t.(map[string]interface{}); ok {
		//	for key, value := range tag {
		//		tags = append(tags, model.Tag{Name: key, Value: toString(value)})
		//	}
		//}
		tag := t.(model.Tag)
		tags = append(tags, model.Tag{Name: tag.Name, Value: tag.Value})
	}

	// Return the converted Item
	return model.Item{
		SiteCode:      s.SiteCode,
		SensorID:      s.SensorID,
		Name:          s.StreamName,
		Process:       s.Process,
		MinValue:      strconv.Itoa(s.MinValue),
		MaxValue:      strconv.Itoa(s.MaxValue),
		UOM:           s.UOM,
		SiteShortCode: getValueForKey(s.Tags, SiteShortCode),
		//System:               mapSystemFromRegistryType(s.RegistryType),
		EquipmentUnit:        getValueForKey(s.Tags, EquipmentType), //TODO check this one
		Subunit:              getValueForKey(s.Tags, "Subunit"),
		EquipmentComponent:   getValueForKey(s.Tags, EquipmentComponent),
		EquipmentMeasurement: getValueForKey(s.Tags, EquipmentMeasurement),
		UDE:                  getValueForKey(s.Tags, UDE),
		SAPEquipmentID:       getValueForKey(s.Tags, SAPEquipmentID),
		Tags:                 tags,
	}
}

// Helper function to safely convert an interface{} to a string.
func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func getValueForKey(tags []interface{}, key string) string {
	var res []string
	for _, t := range tags {
		if tag, ok := t.(model.Tag); ok {
			if tag.Name == key {
				res = append(res, tag.Value)
			}
		}
	}
	return strings.Join(res, ",")
}

// getStringFormTags converts a list of tags represented as []interface{} into a single comma-separated string.
func getStringFormTags(data []interface{}) string {
	var res []string

	for _, tag := range data {
		res = append(res, tag.(model.Tag).Value)
	}
	return strings.Join(res, ",")
}
