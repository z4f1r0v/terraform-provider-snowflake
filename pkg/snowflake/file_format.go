package snowflake

import (
	"github.com/jmoiron/sqlx"
)

func FileFormat(name string) *Builder {
	return &Builder{
		entityType: FileFormatType,
		name:       name,
	}
}

type fileFormat struct {
}

func ScanFileFormat(row *sqlx.Row) (*user, error) {
	r := &user{}
	err := row.StructScan(r)
	return r, err
}
