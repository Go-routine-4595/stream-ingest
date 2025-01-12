package model

type Item struct {
	SiteCode             string // Code for the site
	SensorID             string // Identifier of the sensor
	Name                 string // Name of the entity
	Process              string // Process name
	MinValue             string // Minimum value
	MaxValue             string // Maximum value
	UOM                  string // Unit of measurement
	SiteShortCode        string // Shortened site code
	System               string // System name
	EquipmentUnit        string // Equipment unit name
	Subunit              string // Name of the subunit
	EquipmentComponent   string // Component of the equipment
	EquipmentMeasurement string // Measurement of the equipment
	UDE                  string // UDE (User-Defined Element)
	SAPEquipmentID       string // SAP Equipment ID
	Tags                 []Tag  // all other tag we don't know yet
}

type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
