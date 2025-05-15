// Package constant
// -----------------------------------------------------------------------------
// File: constanthelper.go
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
	"fmt"
)

// CompareConstant compares two Stream objects and returns true if they are identical, otherwise false.
func CompareConstant(c1 Constant, c2 Constant) bool {
	// CompareTo field by field
	// excluding index
	return c1.RegistryType == c2.RegistryType &&
		c1.SiteCode == c2.SiteCode &&
		c1.Value == c2.Value &&
		c1.Process == c2.Process &&
		c1.ConstantName == c2.ConstantName &&
		base.CompareString(c1.UOM, c2.UOM) &&
		base.CompareNumValue(c1.Precision, c2.Precision) &&
		base.CompareNumValue(c1.MinValue, c2.MinValue) &&
		base.CompareNumValue(c1.MaxValue, c2.MaxValue) &&
		base.CompareNumValue(c1.LoLo, c2.LoLo) &&
		base.CompareNumValue(c1.Lo, c2.Lo) &&
		base.CompareNumValue(c1.Hi, c2.Hi) &&
		base.CompareNumValue(c1.HiHi, c2.HiHi) &&
		base.CompareTags(c1.Tags, c2.Tags)
}

// UpdateConstant updates the fields of constant1 with the fields of constant2.
// and UpdateTag set the modifying by/and date
// Constant name is considered as the primary key and won't be changed
func UpdateConstant(c1 *Constant, c2 *Constant, user string) {
	c1.Process = base.CopyString(c1.Process, c2.Process)
	c1.Value = base.CopyString(c1.Value, c2.Value)
	c1.UOM = base.CopyString(c1.UOM, c2.UOM)
	c1.Precision = base.CopyNumValue(c1.Precision, c2.Precision)
	c1.MinValue = base.CopyNumValue(c1.MinValue, c2.MinValue)
	c1.MaxValue = base.CopyNumValue(c1.MaxValue, c2.MaxValue)
	c1.LoLo = base.CopyNumValue(c1.LoLo, c2.LoLo)
	c1.Lo = base.CopyNumValue(c1.Lo, c2.Lo)
	c1.Hi = base.CopyNumValue(c1.Hi, c2.Hi)
	c1.HiHi = base.CopyNumValue(c1.HiHi, c2.HiHi)
	c1.Tags, _ = base.UpdateTags(c1.Tags, c2.Tags)
	c1.SetUpdateBy(user)
}

// ResynchronizeStream updates the fields of constant1 with the fields of constant2.
// and UpdateTag set the modifying by/and date
func ResynchronizeStream(c1 *Constant, c2 *Constant, user string) {
	UpdateConstant(c1, c2, user)
}
func GetExpectedHeader() []string {
	return expectedHeaders
}

func GetExpectedTags() []string {
	list := base.GetTagList()
	for i := 0; i <= base.MaxAllowedUnknownTag; i++ {
		list = append(list, fmt.Sprintf("unknownTag %d", i))
	}
	return list
}
