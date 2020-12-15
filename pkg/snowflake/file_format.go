package snowflake

import (
	"database/sql"
	"encoding/json"
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

type FileFormatOptions struct {
	RecordDelimiter            *string  `json:"RECORD_DELIMITER"`
	FieldDelimiter             *string  `json:"FIELD_DELIMITER"`
	FileExtension              *string  `json:"FILE_EXTENSION"`
	SkipHeader                 *int     `json:"SKIP_HEADER"`
	DateFormat                 *string  `json:"DATE_FORMAT"`
	TimeFormat                 *string  `json:"TIME_FORMAT"`
	TimestampFormat            *string  `json:"TIMESTAMP_FORMAT"`
	BinaryFormat               *string  `json:"BINARY_FORMAT"`
	Escape                     *string  `json:"ESCAPE"`
	EscapeUnenclosedField      *string  `json:"ESCAPE_UNENCLOSED_FIELD"`
	TrimSpace                  *bool    `json:"TRIM_SPACE"`
	FieldOptionallyEnclosedBy  *string  `json:"FIELD_OPTIONALLY_ENCLOSED_BY"`
	NullIf                     []string `json:"NULL_IF"`
	Compression                *string  `json:"COMPRESSION"`
	ErrorOnColumnCountMismatch *bool    `json:"ERROR_ON_COLUMN_COUNT_MISMATCH"`
	ValidateUtf8               *bool    `json:"VALIDATE_UTF8"`
	SkipBlankLines             *bool    `json:"SKIP_BLANK_LINES"`
	ReplaceInvalidCharacters   *bool    `json:"REPLACE_INVALID_CHARACTERS"`
	EmptyFieldAsNull           *bool    `json:"EMPTY_FIELD_AS_NULL"`
	SkipByteOrderMark          *bool    `json:"SKIP_BYTE_ORDER_MARK"`
	Encoding                   *string  `json:"ENCODING"`
}

type fileFormat struct {
	Database            sql.NullString `db:"database_name"`
	Schema              sql.NullString `db:"schema_name"`
	Name                sql.NullString `db:"name"`
	TType               sql.NullString `db:"type"`
	Owner               sql.NullString `db:"owner"`
	Comment             sql.NullString `db:"comment"`
	FormatOptions       sql.NullString `db:"format_options"`
	ParsedFormatOptions *FileFormatOptions
}

func ScanFileFormat(row *sqlx.Row) (*fileFormat, error) {
	f := &fileFormat{}
	err := row.StructScan(f)
	if err != nil {
		return nil, err
	}
	f.ParsedFormatOptions = &FileFormatOptions{}

	err = json.Unmarshal([]byte(f.FormatOptions.String), f.ParsedFormatOptions)

	return f, err
}
