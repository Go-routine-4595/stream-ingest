package base

import (
	"math"
	"testing"
	"time"

	"fmi/stream-ingest/domain/definition"
)

func TestIsProcess(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"validCNCCrushConvey", definition.CNCCrushConvey, true},
		{"invalidProcessName", "InvalidProcess", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsProcess(tt.input)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsTag(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"validEquipmentName", definition.EquipmentName, true},
		{"invalidTagName", "TagNameUnknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsTag(tt.input)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsSiteCodeValid(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"validCode", "CVE", true},
		{"invalidCode", "InvalidSite", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSiteCodeValid(tt.input)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestIsTagConstraintValid(t *testing.T) {
	tests := []struct {
		name          string
		tagName       string
		tagValue      string
		expectedError string
	}{
		{"validNumber", definition.SAPEquipmentID, "12345", ""},
		{"invalidNumber", definition.SAPEquipmentID, "123a", "SAP EquipmentID: 123a is not a number"},
		{"invalidNumber", definition.SAPEquipmentID, "1E+11", "SAP EquipmentID: 1E+11 is not a number"},
	}
	constraintTags[definition.SAPEquipmentID] = constraintType{tagType: "number", repeats: false}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IsTagConstraintTypeValid(tt.tagName, tt.tagValue)
			if err != nil && err.Error() != tt.expectedError {
				t.Errorf("expected %v, got %v", tt.expectedError, err)
			}
		})
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nilValue", nil, ""},
		{"stringValue", "test", "test"},
		{"intValue", 42, "42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToString(tt.input)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestFormatUtcTimestamp(t *testing.T) {
	timestamp := time.Date(2023, 10, 10, 15, 0, 0, 0, time.UTC)
	result := FormatUtcTimestamp(timestamp)
	expected := "2023-10-10T15:00:00.000000Z"
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestUpdateTags(t *testing.T) {
	tests := []struct {
		name          string
		tags1         []Tag
		tags2         []Tag
		expected      []Tag
		expectedError string
	}{
		{"equalTags", []Tag{{Name: "a", Value: "1"}}, []Tag{{Name: "a", Value: "1"}}, []Tag{{Name: "a", Value: "1,1"}}, ""},
		{"unequalTags", []Tag{{Name: "a", Value: "1"}}, []Tag{{Name: "b", Value: "2"}}, []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}, ""},
		{"unequalTags", []Tag{{Name: "a", Value: "1"}}, []Tag{{Name: "a", Value: "2"}}, []Tag{{Name: "a", Value: "1,2"}}, ""},
		{"unequalTags", []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}}, []Tag{{Name: "b", Value: "2"}}, []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "1,2"}}, ""},
		{"unequalTags", []Tag{{Name: "a", Value: "1"}}, []Tag{{Name: "b", Value: "2"}, {Name: "b", Value: "1"}}, []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "b", Value: "1"}}, ""},
		{"unequalTags", []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}}, []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "b", Value: "3"}}, []Tag{{Name: "a", Value: "1,1"}, {Name: "b", Value: "1,2,3"}}, ""},
		{"unequalTags", []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}, {Name: "b", Value: "1"}}, []Tag{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}}, []Tag{{Name: "a", Value: "1,1"}, {Name: "b", Value: "1"}, {Name: "b", Value: "2,2"}}, "duplicate tag name b"},
		{"unequalTags", []Tag{{Name: definition.UDE, Value: "1"}}, []Tag{{Name: definition.UDE, Value: "2"}}, []Tag{{Name: definition.UDE, Value: "2"}}, ""},
	}

	for index, tt := range tests {
		result, err := UpdateTags(tt.tags1, tt.tags2)
		if err != nil {
			if err.Error() != tt.expectedError {
				t.Errorf("test set: %d expected valid=%v, error=%s; got valid=%v, error=%v", index, tt.expected, tt.expectedError, result, err)
				continue
			}
			continue
		}
		for i, tag := range result {
			if tag != tt.expected[i] {
				t.Errorf("test set: %d expected %v, got %v", index, tt.expected[i], tag)
			}
		}
	}
}

func TestCompareTags(t *testing.T) {
	tags1 := []Tag{{Name: "A", Value: "1"}}
	tags2 := []Tag{{Name: "A", Value: "1"}}
	if !CompareTags(tags1, tags2) {
		t.Error("tags should be equal")
	}
	tags2 = []Tag{{Name: "A", Value: "2"}}
	if CompareTags(tags1, tags2) {
		t.Error("tags should not be equal")
	}
}

func TestCopyNumValue(t *testing.T) {
	tests := []struct {
		name     string
		s1       float32
		s2       float32
		expected float32
	}{
		{"validCopy", 5, 10, 10},
		{"nanCopy", 5, float32(math.NaN()), 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CopyNumValue(tt.s1, tt.s2)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetFloatValueFrom(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    float32
		expectError bool
	}{
		{"emptyString", "", float32(math.NaN()), false},
		{"validFloat", "42.5", 42.5, false},
		{"invalidFloat", "abc", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _ := GetFloatValueFrom(tt.input)
			if tt.input != "" && math.IsNaN(float64(result)) {
				t.Errorf("expected %v, got %v", math.NaN(), result)
				return
			}
			if tt.input == "" && math.IsNaN(float64(result)) {
				// specific use case we cant compare if result = NaN
				// result != tt.expected will fail if both are NaN
				// as per Go we
				return
			}
			if result != tt.expected && !tt.expectError {
				t.Errorf("expected %v, got %v", tt.expected, result)
				return
			}
		})
	}
}
