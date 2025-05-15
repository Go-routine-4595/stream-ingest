package dataprocessor

import (
	"os"
	"testing"
)

func TestCSVPersist_Close(t *testing.T) {
	var err error
	instance1, err := NewCSVPersist("/Users/christophebuffard/GolandProjects/stream-ingest/test/test.csv", []string{"col1"}, 10)
	if err != nil {
		t.Fatalf("failed to initialize CSVPersist: %v", err)
	}
	instance2, err := NewCSVPersist("/Users/christophebuffard/GolandProjects/stream-ingest/test/test2.csv", []string{"col1"}, 10)
	if err != nil {
		t.Fatalf("failed to initialize CSVPersist: %v", err)
	}
	tests := []struct {
		name       string
		instance   *CSVPersist
		used       bool
		wantErr    bool
		expectFile bool
	}{
		{"close_unused_file", instance1, false, false, false},
		{"close_used_file", instance2, true, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.used {
				tt.instance.AddRow([]string{"test"})
			}
			err := tt.instance.Close()
			if (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
			if _, exists := os.Stat(tt.instance.fileName); (exists == nil) != tt.expectFile {
				t.Errorf("Close() file existence = %v, expectFile %v", exists == nil, tt.expectFile)
			}
		})
	}
}

func TestCSVPersist_AddRow(t *testing.T) {
	instance, err := NewCSVPersist("test.csv", []string{"col1"}, 10)
	if err != nil {
		t.Fatalf("failed to initialize CSVPersist: %v", err)
	}

	tests := []struct {
		name      string
		row       []string
		instance  *CSVPersist
		expectErr bool
	}{
		{
			"add_valid_row", []string{"row1_a, row1_b"}, instance, false,
		},
		{
			"add_invalid_writer", []string{"test1", "test2"}, instance, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.instance.AddRow(tt.row)
			if (err != nil) != tt.expectErr {
				t.Errorf("AddRow() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestCSVPersist_AddRows(t *testing.T) {
	instance, err := NewCSVPersist("test.csv", []string{"col1", "col2"}, 10)
	if err != nil {
		t.Fatalf("failed to initialize CSVPersist: %v", err)
	}

	tests := []struct {
		name      string
		rows      [][]string
		instance  *CSVPersist
		expectErr bool
	}{
		{
			"add_valid_rows", [][]string{{"row1"}, {"row2"}}, instance, false,
		},
		{
			"add_invalid_rows_1", [][]string{{"row1"}, {"row2"}, {"row3"}}, instance, false,
		},
		{
			"add_invalid_rows_2", [][]string{{"row1_a, row1_b"}}, instance, false,
		},
		{
			"add_invalid_rows_2", [][]string{{"row1_a, row1_b, row1_c"}}, instance, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.instance.AddRows(tt.rows)
			if (err != nil) != tt.expectErr {
				t.Errorf("AddRows() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}

func TestNewCSVPersist(t *testing.T) {
	tests := []struct {
		name           string
		fileName       string
		headers        []string
		flushFrequency int
		expectErr      bool
	}{
		{"valid_persist", "test.csv", []string{"col1", "col2"}, 5, false},
		{"invalid_filename", "", []string{}, 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCSVPersist(tt.fileName, tt.headers, tt.flushFrequency)
			if (err != nil) != tt.expectErr {
				t.Errorf("NewCSVPersist() error = %v, wantErr %v", err, tt.expectErr)
			}
			if !tt.expectErr && (got == nil || got.fileName != tt.fileName) {
				t.Errorf("NewCSVPersist() got = %v, want fileName = %v", got, tt.fileName)
			}
		})
	}
}
