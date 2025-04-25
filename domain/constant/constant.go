// Package constant
// -----------------------------------------------------------------------------
// File: constant.go
// Description: This file implements the CLI command(s) for ingesting stream
//				into FCTS.
//
//	            constant is the main component, it represents a constant FCTS canonical
//	            data model in CosmoDB
//
// Author: <Christophe Buffard>
// Created: <02/28/2025>
// -----------------------------------------------------------------------------
// Notes:
//   - This file is part of the FCTS/stream ingestion project.
//   - Updated/reliable documentation and usage examples can be found at:
//     <Link to project README or documentation>
//
// -----------------------------------------------------------------------------

package constant

import (
	"fmi/stream-ingest/domain/base"
	"fmi/stream-ingest/domain/definition"
)

// ExpectedHeaders defines the list of strings representing the expected header names in a data processing context.
var expectedHeaders = []string{
	definition.CsvSiteCode,
	definition.CsvConstantName,
	definition.CsvValue,
	definition.CsvProcess,
	definition.CsvMinValue,
	definition.CsvMaxValue,
	definition.CsvLoLo,
	definition.CsvLo,
	definition.CsvHi,
	definition.CsvHiHi,
	definition.CsvUom,
}

type Constant struct {
	*base.Base
	RegistryType string `json:"registryType"`
	ConstantName string `json:"streamName"`
	Value        string `json:"value"`
}

func NewConstant(user string) *Constant {
	return &Constant{
		Base:         base.NewBase(user),
		RegistryType: "constant",
		ConstantName: "",
		Value:        "",
	}
}

// GetID returns the unique identifier (ID) of the Constant instance.
// implement the cosmos.Batcher interface
func (c *Constant) GetID() string {
	return c.ConstantName
}

// GetSiteCode returns the SiteCode associated with the Constant instance.
// implement the cosmos.Batcher interface
func (c *Constant) GetSiteCode() string {
	return c.SiteCode
}

// ToRow converts the Stream object into a slice of strings, representing its fields and flattened tags.
// can be used to store in a CSV file
func (c *Constant) ToRow() []string {
	var row []string

	row = append(row, c.SiteCode)
	row = append(row, c.ConstantName)
	row = append(row, c.Value)
	row = append(row, c.Process)
	row = append(row, base.ToString(c.MinValue))
	row = append(row, base.ToString(c.MaxValue))
	row = append(row, base.ToString(c.LoLo))
	row = append(row, base.ToString(c.Lo))
	row = append(row, base.ToString(c.Hi))
	row = append(row, base.ToString(c.HiHi))
	row = append(row, c.UOM)
	row = append(row, c.ToRowWithTags()...)

	return row
}

// Mapper maps the provided CSV header and value to the respective fields in the Constant instance.
// It returns an error if mapping the base fields fails or if no matching headers are found.
func (c *Constant) Mapper(header string, value string) error {

	switch header {
	case definition.CsvValue:
		c.Value = value
		return nil
	case definition.CsvConstantName:
		c.ConstantName = value
		return nil
	}
	return c.Base.Mapper(header, value)
}

// ProcessNumericalValue processes and manipulates the numerical value associated with the Constant instance.
func (c *Constant) ProcessNumericalValue() {
	c.Base.ProcessNumericalValue()
}

// CompareTo compares two Stream objects and returns true if they are identical, otherwise false.
func (c *Constant) CompareTo(other any) bool {
	// CompareTo field by field
	// excluding index
	c2, ok := other.(*Constant)
	if !ok {
		return false
	}
	return c.Base.CompareTo(*c2.Base) &&
		c.Value == c2.Value &&
		c.ConstantName == c2.ConstantName
}

// UpdateWith updates the fields of constant1 with the fields of constant2.
// and UpdateTag set the modifying by/and date
// Constant name is considered as the primary key and won't be changed
func (c *Constant) UpdateWith(other any, user string) {
	c2, ok := other.(*Constant)
	if !ok {
		return
	}
	c.Base.UpdateWith(*c2.Base, user)
	c.Value = base.CopyString(c.Value, c2.Value)
	c.ConstantName = base.CopyString(c.ConstantName, c2.ConstantName)
	c.SetUpdateBy(user)
}

func (c *Constant) Delete(user string) {
	c.SetUpdateBy(user)
	c.Status = "deleted"
}

func (c *Constant) GetStatus() string {
	return c.Status
}

// DeepCopy creates a deep copy of the Constant structure.
// It returns a new Constant instance with all fields copied.
func (c *Constant) DeepCopy() *Constant {
	if c == nil {
		return nil
	}

	return &Constant{
		Base:         c.Base.DeepCopy(), // Assuming Base has a DeepCopy method
		RegistryType: c.RegistryType,    // Strings are immutable, so direct copy is fine
		ConstantName: c.ConstantName,
		Value:        c.Value,
	}
}
