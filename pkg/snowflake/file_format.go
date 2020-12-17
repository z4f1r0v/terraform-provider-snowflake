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
		databaseName:  &b.database,
		schemaName:    &b.schema,
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

func (b FileFormatBuilder) Drop() string {
	return b.builder().Drop()
}

type FileFormatOptionType string

const (
	OptionTypeString      = "string"
	OptionTypeBool        = "bool"
	OptionTypeInt         = "int"
	OptionTypeStringSlice = "[]string"
)

type TypeFileFormatOption struct {
	Type FileFormatOptionType

	Reader func(*FileFormatOptions) interface{}
}

// the format options are returned from snowflake as a json blob, we parse them into a struct in
// pkg/snowflake and use these mappings to extract fields out into the terraform state
// some of the format options are format specific
var FileFormatTypeOptions = map[string]map[string]TypeFileFormatOption{
	"csv": {
		"compression": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.Compression },
		},
		"record_delimiter": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.RecordDelimiter },
		},
		"field_delimiter": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.FieldDelimiter },
		},
		"file_extension": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.FileExtension },
		},
		"trim_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.TrimSpace },
		},
		"skip_header": {
			Type:   OptionTypeInt,
			Reader: func(o *FileFormatOptions) interface{} { return o.SkipHeader },
		},
		"skip_blank_lines": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.SkipBlankLines },
		},
		"date_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.DateFormat },
		},
		"time_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.TimeFormat },
		},
		"timestamp_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.TimestampFormat },
		},
		"binary_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.BinaryFormat },
		},
		"escape": {
			Type: OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} {
				t := o.Escape
				if t != nil && *t == "NONE" {
					return nil
				}
				return t
			},
		},
		"escape_unenclosed_field": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.EscapeUnenclosedField },
		},
		"field_optionally_enclosed_by": {
			Type: OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} {
				t := o.FieldOptionallyEnclosedBy
				if t != nil && *t == "NONE" {
					return nil
				}
				return t
			},
		},
		"error_on_column_count_mismatch": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.ErrorOnColumnCountMismatch },
		},
		"replace_invalid_characters": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.ReplaceInvalidCharacters },
		},
		"validate_utf8": {
			Type: OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} {
				fmt.Printf("[DEBUG] YYY utf8 %#v \n", *o.ValidateUtf8)
				return o.ValidateUtf8
			},
		},
		"empty_field_as_null": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.EmptyFieldAsNull },
		},
		"skip_byte_order_mark": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.SkipByteOrderMark },
		},
		"encoding": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.Encoding },
		},
		"null_if": {
			Type:   OptionTypeStringSlice,
			Reader: func(o *FileFormatOptions) interface{} { return o.NullIf },
		},
	},
	"json": {
		"compression": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.Compression },
		},
		"date_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.DateFormat },
		},
		"time_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.TimeFormat },
		},
		"timestamp_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.TimestampFormat },
		},
		"binary_format": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.BinaryFormat },
		},
		"null_if": {
			Type:   OptionTypeStringSlice,
			Reader: func(o *FileFormatOptions) interface{} { return o.NullIf },
		},
		"file_extension": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.FileExtension },
		},
		"replace_invalid_characters": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.ReplaceInvalidCharacters },
		},
		"skip_byte_order_mark": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.SkipByteOrderMark },
		},
		"enable_octal": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.EnableOctal },
		},
		"allow_duplicate": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.AllowDuplicate },
		},
		"strip_outer_array": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.StripOuterArray },
		},
		"strip_null_values": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.StripNullValues },
		},
		"ignore_utf8_errors": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.IgnoreUtf8Errors },
		},
		"trim_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.TrimSpace },
		},
	},
	"avro": {
		"null_if": {
			Type:   OptionTypeStringSlice,
			Reader: func(o *FileFormatOptions) interface{} { return o.NullIf },
		},
		"compression": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.Compression },
		},
		"trim_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.TrimSpace },
		},
	},
	"orc": {
		"trim_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.TrimSpace },
		},
		"null_if": {
			Type:   OptionTypeStringSlice,
			Reader: func(o *FileFormatOptions) interface{} { return o.NullIf },
		},
	},
	"parquet": {
		"compression": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.Compression },
		},
		"trim_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.TrimSpace },
		},
		"binary_as_text": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.BinaryAsText },
		},
		"null_if": {
			Type:   OptionTypeStringSlice,
			Reader: func(o *FileFormatOptions) interface{} { return o.NullIf },
		},
	},
	"xml": {
		"compression": {
			Type:   OptionTypeString,
			Reader: func(o *FileFormatOptions) interface{} { return o.Compression },
		},
		"trim_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.TrimSpace },
		},
		"ignore_utf8_errors": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.IgnoreUtf8Errors },
		},
		"null_if": {
			Type:   OptionTypeStringSlice,
			Reader: func(o *FileFormatOptions) interface{} { return o.NullIf },
		},
		"skip_byte_order_mark": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.SkipByteOrderMark },
		},
		"preserve_space": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.PreserveSpace },
		},
		"strip_outer_element": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.StripOuterElement },
		},
		"disable_snowflake_data": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.DisableSnowflakeData },
		},
		"disable_auto_convert": {
			Type:   OptionTypeBool,
			Reader: func(o *FileFormatOptions) interface{} { return o.DisableAutoConvert },
		},
	},
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
	EnableOctal                *bool    `json:"ENABLE_OCTAL"`
	AllowDuplicate             *bool    `json:"ALLOW_DUPLICATE"`
	StripOuterArray            *bool    `json:"STRIP_OUTER_ARRAY"`
	StripNullValues            *bool    `json:"STRIP_NULL_VALUES"`
	IgnoreUtf8Errors           *bool    `json:"IGNORE_UTF8_ERRORS"`
	BinaryAsText               *bool    `json:"BINARY_AS_TEXT"`
	PreserveSpace              *bool    `json:"PRESERVE_SPACE"`
	StripOuterElement          *bool    `json:"STRIP_OUTER_ELEMENT"`
	DisableSnowflakeData       *bool    `json:"DISABLE_SNOWFLAKE_DATA"`
	DisableAutoConvert         *bool    `json:"DISABLE_AUTO_CONVERT"`
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
	fmt.Printf("[DEBUG] ZZZ parsed options %#v \n", f.FormatOptions.String)
	err = json.Unmarshal([]byte(f.FormatOptions.String), f.ParsedFormatOptions)

	return f, err
}
