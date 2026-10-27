// Package migrations embeds the SQL migrations. Files are named NNNN_name.sql,
// numbered from 0001 without gaps, and applied in order. They are forward-only
// and immutable once released: fix mistakes with a new migration.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
