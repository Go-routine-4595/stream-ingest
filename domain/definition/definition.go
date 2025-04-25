// Package stream
// -----------------------------------------------------------------------------
// File: definition.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				definition of constant specif to the project/domain
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

package definition

// CSV Tags
const (
	CsvSiteCode     = "SiteCode"
	CsvProcess      = "Process"
	CsvStreamName   = "StreamName"
	CsvSensorId     = "SensorId"
	CsvUom          = "Uom"
	CsvScaleFactor  = "ScaleFactor"
	CsvMinValue     = "MinValue"
	CsvMaxValue     = "MaxValue"
	CsvLoLo         = "LoLo"
	CsvLo           = "Lo"
	CsvHi           = "Hi"
	CsvHiHi         = "HiHi"
	CsvValue        = "Value"
	CsvConstantName = "ConstantName"
)

// Error
const (
	ErrBaseInvalidScaleFactor   = "scale factor is not a number"
	ErrBaseInvalidProcess       = "invalid process"
	ErrBaseInvalidMinValue      = "minValue is not a number"
	ErrBaseInvalidMaxValue      = "maxValue is not a number"
	ErrBaseInvalidLoLo          = "loLo is not a number"
	ErrBaseInvalidLo            = "lo is not a number"
	ErrBaseInvalidHi            = "hi is not a number"
	ErrBaseInvalidHiHi          = "hiHi is not a number"
	ErrBaseInvalidSiteCode      = "wrong siteCode "
	ErrBaseInvalidTagConstraint = "invalid tag constraint"
)

// Tags
const (
	CollarElevation      = "CollarElevation"
	EquipmentClass       = "EquipmentClass"
	EquipmentComponent   = "EquipmentComponent"
	EquipmentMeasurement = "EquipmentMeasurement"
	EquipmentName        = "EquipmentName"
	EquipmentSubUnit     = "EquipmentSubUnit"
	EquipmentType        = "EquipmentType"
	EquipmentUnit        = "EquipmentUnit"
	GaugeFactor          = "GaugeFactor"
	GPSLatitude          = "GPSLatitude"
	GPSLongitude         = "GPSLongitude"
	Interpolation        = "Interpolation"
	OpStatsLoader        = "OpStatsLoader"
	SAPEquipmentID       = "SAPEquipmentID"
	SAPMeasurementID     = "SAPMeasurementID"
	SAPMeasurementType   = "SAPMeasurementType"
	SAPUOM               = "SAPUOM"
	Scaling              = "Scaling"
	SensorElevation      = "SensorElevation"
	SIMS                 = "SIMS"
	System               = "System"
	UDE                  = "UDE"
	Workflow             = "Workflow"
	ZeroReading          = "ZeroReading"
	SiteShortCode        = "SAPSiteCode"
	OEM                  = "OEM"
	Severity             = "Severity"
	EquipmentId          = "EquipmentId"
	Delay                = "Delay"
)

var TagsMap = map[string]string{
	CollarElevation:      CollarElevation,
	EquipmentClass:       EquipmentClass,
	EquipmentComponent:   EquipmentComponent,
	EquipmentMeasurement: EquipmentMeasurement,
	EquipmentName:        EquipmentName,
	EquipmentSubUnit:     EquipmentSubUnit,
	EquipmentType:        EquipmentType,
	EquipmentUnit:        EquipmentUnit,
	GaugeFactor:          GaugeFactor,
	GPSLatitude:          GPSLatitude,
	GPSLongitude:         GPSLongitude,
	Interpolation:        Interpolation,
	OpStatsLoader:        OpStatsLoader,
	SAPEquipmentID:       SAPEquipmentID,
	SAPMeasurementID:     SAPMeasurementID,
	SAPMeasurementType:   SAPMeasurementType,
	SAPUOM:               SAPUOM,
	Scaling:              Scaling,
	SensorElevation:      SensorElevation,
	SIMS:                 SIMS,
	System:               System,
	UDE:                  UDE,
	Workflow:             Workflow,
	ZeroReading:          ZeroReading,
	SiteShortCode:        SiteShortCode, // Matches SAPSiteCode
	OEM:                  OEM,
	Severity:             Severity,
	EquipmentId:          EquipmentId,
	Delay:                Delay,
}

var TagsMapIndex = map[string]uint{
	CollarElevation:      0,
	EquipmentClass:       1,
	EquipmentComponent:   2,
	EquipmentMeasurement: 3,
	EquipmentName:        4,
	EquipmentSubUnit:     5,
	EquipmentType:        6,
	EquipmentUnit:        7,
	GaugeFactor:          8,
	GPSLatitude:          9,
	GPSLongitude:         10,
	Interpolation:        11,
	OpStatsLoader:        12,
	SAPEquipmentID:       13,
	SAPMeasurementID:     14,
	SAPMeasurementType:   15,
	SAPUOM:               16,
	Scaling:              17,
	SensorElevation:      18,
	SIMS:                 19,
	System:               20,
	UDE:                  21,
	Workflow:             22,
	ZeroReading:          23,
	SiteShortCode:        24,
	OEM:                  25,
	Severity:             26,
	EquipmentId:          27,
	Delay:                28,
}

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
	Milling                 = "MIL"
)

// Process is a map that associates process keys with their corresponding identifiers as string constants.
var Process map[string]string = map[string]string{
	CNCCrushConvey:          CNCCrushConvey,
	ENVEnvironmental:        ENVEnvironmental,
	GDEEquipmentManagement:  GDEEquipmentManagement,
	FTCFMCTechnologyCenter:  FTCFMCTechnologyCenter,
	FNLFragmentationLoading: FNLFragmentationLoading,
	General:                 General,
	Haulage:                 Haulage,
	HM:                      HM,
	Leaching:                Leaching,
	MN:                      MN,
	MO:                      MO,
	MIS:                     MIS,
	REF:                     REF,
	ROD:                     ROD,
	SMLSmelting:             SMLSmelting,
	TCLWTCLW:                TCLWTCLW,
	GMX:                     GMX,
	SAMEM:                   SAMEM,
	NAMEM:                   NAMEM,
	Milling:                 Milling,
}

// SiteCode
const (
	BAG   = "BAG"
	MOR   = "MOR"
	CVE   = "CVE"
	CMX   = "CMX"
	HEN   = "HEN"
	SIE   = "SIE"
	SAM   = "SAM"
	MIA   = "MIA"
	NMO   = "MNO"
	ABR   = "ABR"
	SAMEM = "SAMEM"
	NAMEM = "NAMEM"
)

const (
	ConstraintSAPID       = "sapid"
	ConstraintNumber      = "number"
	ConstraintString      = "string"
	ConstraintDate        = "date"
	ConstraintTime        = "time"
	ConstraintBool        = "bool"
	ConstraintEnum        = "enum"
	ConstraintArray       = "array"
	ConstraintObject      = "object"
	ConstrainDecimalPoint = "decimalPoint"
	ConstraintAny         = "any"
)

// SiteCode is a map that associates specific string keys with their corresponding string values representing site codes.
var SiteCode map[string]string = map[string]string{
	BAG:   BAG,
	MOR:   MOR,
	CVE:   CVE,
	CMX:   CMX,
	HEN:   HEN,
	SIE:   SIE,
	SAM:   SAM,
	MIA:   MIA,
	NMO:   NMO,
	ABR:   ABR,
	SAMEM: SAMEM,
	NAMEM: NAMEM,
}
