package snowflake

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// FileFormat returns a pointer to a Builder for a database
func FileFormat(name string) *Builder {
	return &Builder{
		name:       name,
		entityType: FileFormatType,
	}
}

type fileFormat struct {
	Name    sql.NullString `db:"name"`
	Comment sql.NullString `db:"comment"`
}

func ScanFileFormat(row *sqlx.Row) (*fileFormat, error) {
	f := &fileFormat{}
	e := row.StructScan(f)
	return f, e
}

// func ListFileFormats(sdb *sqlx.DB) ([]database, error) {
// 	stmt := "SHOW DATABASES"
// 	rows, err := sdb.Queryx(stmt)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	dbs := []database{}
// 	err = sqlx.StructScan(rows, &dbs)
// 	if err == sql.ErrNoRows {
// 		log.Printf("[DEBUG] no databases found")
// 		return nil, nil
// 	}
// 	return dbs, errors.Wrapf(err, "unable to scan row for %s", stmt)
// }
