package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// FileFormat returns a pointer to a Builder for a database
func FileFormat(database, schema, name string) *FileFormatBuilder {
	return &FileFormatBuilder{
		database: database,
		schema:   schema,
		name:     name,
	}
}

type FileFormatBuilder struct {
	database string
	schema   string
	name     string
}

func (b FileFormatBuilder) Show() string {
	return fmt.Sprintf(
		`SHOW FILE FORMATS LIKE '%s' in SCHEMA %s.%s`, b.name, b.database, b.schema)
}

// QualifiedName prepends the db and schema if set and escapes everything nicely
func (b FileFormatBuilder) qualifyName(database, schema, fileFormat string) string {
	var n strings.Builder

	if database != "" && schema != "" {
		n.WriteString(fmt.Sprintf(`"%v"."%v".`, database, schema))
	}

	if database != "" && schema == "" {
		n.WriteString(fmt.Sprintf(`"%v"..`, database))
	}

	if database == "" && schema != "" {
		n.WriteString(fmt.Sprintf(`"%v".`, schema))
	}

	n.WriteString(fmt.Sprintf(`"%v"`, fileFormat))

	return n.String()
}

func (b FileFormatBuilder) QualifiedName() string {
	return b.qualifyName(b.database, b.schema, b.name)
}

func (b FileFormatBuilder) builder() *Builder {
	return &Builder{
		entityType:    FileFormatType,
		name:          b.QualifiedName(),
		qualifiedName: true,
	}
}

func (b FileFormatBuilder) Create(ttype string) *CreateBuilder {
	c := b.builder().Create()
	c.SetString("TYPE", ttype)

	return c
}

func (b FileFormatBuilder) Rename(newName string) string {
	newQualifiedName := b.qualifyName(b.database, b.schema, newName)
	return b.builder().Rename(newQualifiedName)
}

func (b FileFormatBuilder) Alter() *AlterPropertiesBuilder {
	return b.builder().Alter()
}

type fileFormat struct {
	Database      sql.NullString `db:"database_name"`
	Schema        sql.NullString `db:"schema_name"`
	Name          sql.NullString `db:"name"`
	TType         sql.NullString `db:"type"`
	Owner         sql.NullString `db:"owner"`
	Comment       sql.NullString `db:"comment"`
	FormatOptions sql.NullString `db:"format_options"`
}

func ScanFileFormat(row *sqlx.Row) (*fileFormat, error) {
	f := &fileFormat{}
	e := row.StructScan(f)
	return f, e
}
