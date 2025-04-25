// Package stream
// -----------------------------------------------------------------------------
// File: streamhelper.go
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
	"fmi/stream-ingest/domain/base"
	"fmt"
)

// CompareStreams compares two Stream objects and returns true if they are identical, otherwise false.
func CompareStreams(s1 Stream, s2 Stream) bool {
	// CompareTo field by field
	// excluding index
	return s1.RegistryType == s2.RegistryType &&
		s1.SiteCode == s2.SiteCode &&
		s1.SensorID == s2.SensorID &&
		s1.Process == s2.Process &&
		s1.StreamName == s2.StreamName &&
		base.CompareString(s1.UOM, s2.UOM) &&
		base.CompareNumValue(s1.ScaleFactor, s2.ScaleFactor) &&
		base.CompareNumValue(s1.Precision, s2.Precision) &&
		base.CompareNumValue(s1.MinValue, s2.MinValue) &&
		base.CompareNumValue(s1.MaxValue, s2.MaxValue) &&
		base.CompareNumValue(s1.LoLo, s2.LoLo) &&
		base.CompareNumValue(s1.Lo, s2.Lo) &&
		base.CompareNumValue(s1.Hi, s2.Hi) &&
		base.CompareNumValue(s1.HiHi, s2.HiHi) &&
		base.CompareTags(s1.Tags, s2.Tags)
}

// UpdateStream updates the fields of stream1 with the fields of stream2.
// and UpdateTag set the modifying by/and date
// Sensor sensorId is considered as the primary key and won't be changed
func UpdateStream(s1 *Stream, s2 *Stream, user string) {
	s1.Process = base.CopyString(s1.Process, s2.Process)
	s1.StreamName = base.CopyString(s1.StreamName, s2.StreamName)
	s1.UOM = base.CopyString(s1.UOM, s2.UOM)
	s1.ScaleFactor = base.CopyNumValue(s1.ScaleFactor, s2.ScaleFactor)
	s1.Precision = base.CopyNumValue(s1.Precision, s2.Precision)
	s1.MinValue = base.CopyNumValue(s1.MinValue, s2.MinValue)
	s1.MaxValue = base.CopyNumValue(s1.MaxValue, s2.MaxValue)
	s1.LoLo = base.CopyNumValue(s1.LoLo, s2.LoLo)
	s1.Lo = base.CopyNumValue(s1.Lo, s2.Lo)
	s1.Hi = base.CopyNumValue(s1.Hi, s2.Hi)
	s1.HiHi = base.CopyNumValue(s1.HiHi, s2.HiHi)
	s1.Tags, _ = base.UpdateTags(s1.Tags, s2.Tags)
	s1.SetUpdateBy(user)
}

// ResynchronizeStream updates the fields of stream1 with the fields of stream2.
// and UpdateTag set the modifying by/and date
func ResynchronizeStream(s1 *Stream, s2 *Stream, user string) {
	UpdateStream(s1, s2, user)
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
