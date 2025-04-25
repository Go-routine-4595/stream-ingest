// Package config
// -----------------------------------------------------------------------------
// File: version.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//	            It provides functionalities to interact with the user and
//	            verify the input streams definition file syntax
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
package config

var BuildDate string

const (
	Version = "0.7.7"
	Author  = "Christophe Buffard"
	Email   = "cbuffard@fmi.com"
)
