// Package cosmos
// -----------------------------------------------------------------------------
// File: access.go
// Description: This file implements the CLI command(s) for ingesting stream
//
//				into FCTS.
//
//
//
//	            Access data
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
package cosmos

const (
	accountEndpointP = "https://registery.documents.azure.com:443/"
	databaseNameP    = "registery"
	containerNameP   = "registery"

	accountEndpointDev = "https://fctsnadevlcosdbs01.documents.azure.com:443/"
	databaseNameDev    = "FAE"
	containerNameDev   = "Registry"

	accountEndpointProd = "https://fctsnaprodcosdbs01.documents.azure.com:443/"
	databaseNameProd    = "FAE"
	containerNameProd   = "Registry"
)
