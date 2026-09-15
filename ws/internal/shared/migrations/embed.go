// Package migrations provides embedded database migration files.
package migrations

import "embed"

// Postgres contains all PostgreSQL migration SQL files and the atlas.sum checksum file.
// Used by the provisioning service to apply migrations on startup.
//
//go:embed postgres/*.sql postgres/atlas.sum
var Postgres embed.FS
