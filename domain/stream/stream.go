package stream

import (
	"githb.com/Go-routine-4595/stream-ingest/model"
	"github.com/google/uuid"
	"strconv"
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
	SAPEquipmentID       = "SAP EquipmentID"
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

// ConvertStreamToItem converts a Stream structure to an Item structure.
func (s Stream) ConvertStreamToItem() model.Item {
	// Convert Tags from []interface{} to []Tag
	var tags []model.Tag = make([]model.Tag, len(s.Tags))
	for i, t := range s.Tags {
		var tag model.Tag
		tag.Name = t.(map[string]interface{})["name"].(string)
		tag.Value = t.(map[string]interface{})["value"].(string)
		tags[i] = model.Tag{Name: tag.Name, Value: tag.Value}
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
		// the SAP Equipement ID is defined differently between the csv and the registry...
		SAPEquipmentID: getValueForKey(s.Tags, "SAP Equipment ID"),
		Tags:           tags,
	}
}
