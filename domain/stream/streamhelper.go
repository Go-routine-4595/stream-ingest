package stream

import (
	"fmt"
	"githb.com/Go-routine-4595/stream-ingest/model"
	"reflect"
	"strings"
	"time"
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

// Helper function to safely convert an interface{} to a string.
func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

// Helper function to format UTC time to ISO 8601 with microsecond precision ending in "Z"
func formatUtcTimestamp(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000000Z")
}

func getValueForKey(tags []interface{}, key string) string {
	var res []string
	for _, t := range tags {
		if tag, ok := t.(map[string]interface{}); ok {
			if tag["name"].(string) == key {
				res = append(res, tag["value"].(string))
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

// compareTags is a helper function to compare two slices of interface{} representing tags.
// all tags in tags2 should be in tags1 we want to add missing tag in tags1 from tags2
func compareTags(tags1 []interface{}, tags2 []interface{}) bool {

	// Convert tags slices to maps for easy comparison
	tagMap1 := make(map[model.Tag]bool)
	tagMap2 := make(map[model.Tag]bool)

	for _, tag := range tags1 {
		if t, ok := tag.(map[string]interface{}); ok {
			var v model.Tag
			v.Value = t["value"].(string)
			v.Name = t["name"].(string)
			tagMap1[v] = true
		}
	}

	for _, tag := range tags2 {
		if t, ok := tag.(map[string]interface{}); ok {
			var v model.Tag
			v.Value = t["value"].(string)
			v.Name = t["name"].(string)
			tagMap2[v] = true
		}
	}

	for tag := range tagMap2 {
		if !tagMap1[tag] {
			return false
		}
	}

	return true
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
		var t model.Tag
		t.Value = tag.(map[string]interface{})["value"].(string)
		t.Name = tag.(map[string]interface{})["name"].(string)
		if _, exists := existingTagsSet[t]; !exists {
			var interfaceValue interface{} = tag
			stream1.Tags = append(stream1.Tags, interfaceValue)
		}
	}
	*stream1 = stream1.SetUpdateBy(user)

}

// CompareStreams compares two Stream objects and returns true if they are identical, otherwise false.
func CompareStreams(s1 Stream, s2 Stream) bool {
	// Compare field by field
	return s1.RegistryType == s2.RegistryType &&
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
