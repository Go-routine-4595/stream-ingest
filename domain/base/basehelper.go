// Package base
// -----------------------------------------------------------------------------
// File: basehelper.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//	            helper function for base data
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
	"fmi/stream-ingest/domain/definition"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// IsProcess helper function
func IsProcess(s string) bool {

	_, ok := definition.Process[s]
	return ok
}

// IsTag checks if a provided tag matches one of the defined constants.
func IsTag(tag string) bool {

	_, ok := definition.TagsMap[tag]
	return ok
}

// IsSiteCodeValid checks if the provided siteCode is valid by verifying its presence in the predefined set of codes.
func IsSiteCodeValid(site string) bool {
	d, ok := definition.SiteCode[site]
	_ = d
	return ok
}

// IsTagConstraintTypeValid validates if the given tagValue satisfying the constraint specified by the tagName.
// Returns true if the constraint is met; otherwise, returns false. If the tagName is unknown, it returns true
func IsTagConstraintTypeValid(tagName string, tagValue string) error {
	_, ok := constraintTags[tagName]
	if !ok {
		return nil
	}
	switch constraintTags[tagName].tagType {
	case definition.ConstraintSAPID:
		// to remove leading and tailing space, we want a number no space before and after,
		// and it's there to overcome Excel issue on a big volume of data with SAP number
		tagValue = strings.TrimSpace(tagValue)
		res := isStringDigitsOnly(tagValue)
		if !res {
			return fmt.Errorf("%s: %s is not a number", tagName, tagValue)
		}
	case definition.ConstraintNumber:
		res := isStringDigitsOnly(tagValue)
		if !res {
			return fmt.Errorf("%s: %s is not a number", tagName, tagValue)
		}
	case definition.ConstraintString:
		res := hasLeadingOrTrailingSpace(tagValue)
		if res {
			return fmt.Errorf("%s: %s has leading or trailing space", tagName, tagValue)
		}
	}
	return nil
}

// IsTagConstraintValid checks if a tag constraint allows repeated occurrences in `constraintTags`.
func IsTagConstraintValid(tagName string) bool {
	if _, ok := constraintTags[tagName]; !ok {
		// we always send false for unknown tag
		return false
	}
	return constraintTags[tagName].repeats
}

// hasLeadingOrTrailingSpace checks if a string has leading or trailing spaces.
func hasLeadingOrTrailingSpace(s string) bool {
	trimmed := strings.TrimSpace(s)
	return s != trimmed
}

// isStringDigitsOnly checks if the input string contains only digit characters and returns true if all are digits.
func isStringDigitsOnly(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// ToString Helper function to safely convert an interface{} to a string.
func ToString(value interface{}) string {
	// no value
	if value == nil {
		return ""
	}
	// we didn't have a value in the import csv file
	if s, ok := value.(float32); ok {
		if math.IsNaN(float64(s)) {
			return ""
		}
	}
	return fmt.Sprintf("%v", value)
}

// FormatUtcTimestamp Helper function to format UTC time to ISO 8601 with microsecond precision ending in "Z"
func FormatUtcTimestamp(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000000Z")
}

// CheckForDuplicatedTags verifies that the given slice of tags does not contain duplicate tag names and returns an error if duplicates exist.
func CheckForDuplicatedTags(tags []Tag) error {
	tagMap1 := make(map[string]struct {
		s   string
		pos int
	})
	for _, tag := range tags {
		if _, ok := tagMap1[tag.Name]; ok {
			return fmt.Errorf("duplicate tag name %s", tag.Name)
		}
	}
	return nil
}

// CopyString returns s2 if it is not empty; otherwise, it returns s1.
func CopyString(s1 string, s2 string) string {
	if s2 == "" {
		return s1
	}
	return s2
}

// CopyNumValue returns s2 unless s2 is NaN, in which case it returns s1.
// s2 is the value we want to use unless it's not defined
func CopyNumValue(s1 float32, s2 float32) float32 {
	if math.IsNaN(float64(s2)) {
		return s1
	}
	return s2
}

// GetFloatValueFrom converts a string to a float32 value and returns an error if it fails.
func GetFloatValueFrom(s string) (float32, error) {
	if s == "" {
		return float32(math.NaN()), nil // If the input is empty, treat it as zero
	}

	// Convert the string to a float64 value first (as this is what strconv provides)
	floatVal, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0, errors.New("failed to convert to float32: " + err.Error())
	}

	// Return the float32 representation of the value
	return float32(floatVal), nil
}

// CompareString determines if two strings are equal, returning true if the second string is empty or if both strings match.
func CompareString(s1 string, s2 string) bool {
	if s2 == "" {
		return true
	}
	return s1 == s2
}

// CompareNumValue checks if two float32 values are equal, treating NaN in the second value as a match condition.
// if the CSV file does not provide a numerical value, we use NaN so we can't compare as we will be using the stored
// numerical value
func CompareNumValue(s1 float32, s2 float32) bool {
	if math.IsNaN(float64(s2)) {
		return true
	}
	return s1 == s2
}

// CopyTags copies tags from tags2 to a new slice, effectively returning tags2 as a new slice of Tag structs.
func CopyTags(tags1 []Tag, tags2 []Tag) []Tag {
	return tags2
}

// IsTagInSlice checks if a tag with the specified tagName exists in the given slice of Tag objects.
func IsTagInSlice(tagName string, tags []Tag) bool {
	for _, tag := range tags {
		if tag.Name == tagName {
			return true
		}
	}
	return false
}

// CompareTags is a helper function to compare two slices of Tag representing tags.
// All tags in tags2 should exist in tags1. Returns true if all tags in tags2 are in tags1.
// O(len([]tags1) + len([]tags2))
func CompareTags(tags1 []Tag, tags2 []Tag) bool {
	// Convert tags1 to a map for quick lookup
	tagMap1 := make(map[string]struct {
		s   string
		pos int
	})
	for i, tag := range tags1 {
		tagMap1[tag.Name] = struct {
			s   string
			pos int
		}{tag.Value, i}
	}

	// Iterate through tags2 and add missing tags to tags1
	for _, tag := range tags2 {
		if _, exists := tagMap1[tag.Name]; !exists {
			// we found a missing tag (not in tags1)
			return false
		} else {
			// first we take the index in the []Tag
			index := tagMap1[tag.Name].pos
			// we take the tag.Value of it
			data := tags1[index].Value
			// if Value in tags1 is different from tags2.Value we return
			if data != tag.Value {
				return false
			}
		}

	}
	return true
}

// UpdateTags is a helper function to update two slices of []Tag representing tags.
// All tags in tags2 should exist in tags1. If any tag in tags2 is missing from tags1, we add it to tags1.
// we copy the missing tags in tags1 from tags2 into tags1 as an update
// O(len([]tags1) + len([]tags2))
func UpdateTags(tags1 []Tag, tags2 []Tag) ([]Tag, error) {
	// Convert tags1 to a map for quick lookup
	tagMap1 := make(map[string]struct {
		s   string
		pos int
	})
	for i, tag := range tags1 {
		if _, ok := tagMap1[tag.Name]; ok {
			return tags1, fmt.Errorf("duplicate tag name %s", tag.Name)
		}
		tagMap1[tag.Name] = struct {
			s   string
			pos int
		}{tag.Value, i}
	}

	// Iterate through tags2 and add missing tags to tags1
	for _, tag := range tags2 {
		if _, exists := tagMap1[tag.Name]; !exists {
			// Add missing tag to tags1
			tags1 = append(tags1, tag)
		} else {
			// first we take the index in the []Tag
			index := tagMap1[tag.Name].pos

			if IsTagConstraintValid(tag.Name) {
				// is this tag allowed to have multiple instance = yes we add the value to the tag
				// copy the tag from tags2 in tags1

				// we take the tag.Value of it
				data := tags1[index].Value
				// we add the data in tags2 via the iterator tag over tags2
				data = data + "," + tag.Value
				// and we store the data back to tags1[index]
				tags1[index].Value = data
			} else {
				// no we override it
				tags1[index].Value = tag.Value
			}
		}

	}
	return tags1, nil
}

func GetOEMTagInfo() (string, bool) {
	return constraintTags[definition.OEM].tagType, constraintTags[definition.OEM].repeats
}

func SetOEMTagInfo(repeats bool) {
	constraintTypeVar := constraintTags[definition.OEM]
	constraintTypeVar.repeats = repeats
	constraintTags[definition.OEM] = constraintTypeVar
}

// hashStringToInt my simple hash function that takes a string and produce an int
func hashStringToInt(s string) uint {
	const prime = 31
	const mod = uint(^uint(0) >> 1) // Max value for uint (platform-dependent)
	var hash uint
	for i := 0; i < len(s); i++ {
		hash = (hash*prime + uint(s[i])) % uint(mod)
	}
	return hash
}

// GetTagList retrieves a list of all tag keys from the TagsMap in the definition package.
func GetTagList() []string {
	var (
		tagList []string
	)
	// Extract keys and sort them by the map values (uint)
	tagList = make([]string, 0, len(definition.TagsMapIndex))
	for k := range definition.TagsMapIndex {
		tagList = append(tagList, k)
	}

	// Sort keys based on the associated values
	sort.Slice(tagList, func(i, j int) bool {
		return definition.TagsMapIndex[tagList[i]] < definition.TagsMapIndex[tagList[j]]
	})

	return tagList
}
