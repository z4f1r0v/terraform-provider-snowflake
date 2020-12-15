package resources

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"strings"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/pkg/errors"
)

var fileFormatProperties = []string{
	"type",
	"comment",
}

var fileFormatSchema = map[string]*schema.Schema{
	"database": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Name of the file format.",
	},
	"schema": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Name of the file format.",
	},
	"name": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "Name of the file format.",
	},
	"comment": {
		Type:     schema.TypeString,
		Optional: true,
	},
	"type": {
		Type:     schema.TypeString,
		Computed: true,
	},

	"csv": {
		Type:          schema.TypeSet,
		MaxItems:      1,
		Optional:      true,
		ConflictsWith: []string{"json", "avro", "orc", "parquet", "xml"},
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"compression": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"record_delimiter": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringLenBetween(1, 1),
				},
				"field_delimiter": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringLenBetween(1, 1),
				},
				"file_extension": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"skip_header": {
					Type:     schema.TypeInt,
					Optional: true,
					Computed: true,
				},
				"skip_blank_lines": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},

				//  SKIP_HEADER = <integer>
				//  SKIP_BLANK_LINES = TRUE | FALSE
				//  DATE_FORMAT = '<string>' | AUTO
				//  TIME_FORMAT = '<string>' | AUTO
				//  TIMESTAMP_FORMAT = '<string>' | AUTO
				//  BINARY_FORMAT = HEX | BASE64 | UTF8
				//  ESCAPE = '<character>' | NONE
				//  ESCAPE_UNENCLOSED_FIELD = '<character>' | NONE
				//  TRIM_SPACE = TRUE | FALSE
				//  FIELD_OPTIONALLY_ENCLOSED_BY = '<character>' | NONE
				//  NULL_IF = ( '<string>' [ , '<string>' ... ] )
				//  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE | FALSE
				//  REPLACE_INVALID_CHARACTERS = TRUE | FALSE
				//  VALIDATE_UTF8 = TRUE | FALSE
				//  EMPTY_FIELD_AS_NULL = TRUE | FALSE
				//  SKIP_BYTE_ORDER_MARK = TRUE | FALSE
				//  ENCODING = '<string>' | UTF8
			}},
	},

	"json": {
		Type:          schema.TypeSet,
		MaxItems:      1,
		Optional:      true,
		ConflictsWith: []string{"csv", "avro", "orc", "parquet", "xml"},
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"compression": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
			},
		},
	},

	"avro": {
		Type:          schema.TypeSet,
		MaxItems:      1,
		Optional:      true,
		ConflictsWith: []string{"csv", "json", "orc", "parquet", "xml"},
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"compression": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
			},
		},
	},

	"orc": {
		Type:          schema.TypeSet,
		MaxItems:      1,
		Optional:      true,
		ConflictsWith: []string{"csv", "json", "avro", "parquet", "xml"},
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
			},
		},
	},

	"parquet": {
		Type:          schema.TypeSet,
		MaxItems:      1,
		Optional:      true,
		ConflictsWith: []string{"csv", "json", "avro", "orc", "xml"},
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"compression": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
			},
		},
	},

	"xml": {
		Type:          schema.TypeSet,
		MaxItems:      1,
		Optional:      true,
		ConflictsWith: []string{"csv", "json", "avro", "orc", "parquet"},
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"compression": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
			},
		},
	},
}

type optionType string

const (
	optionTypeString = "string"
	optionTypeBool   = "bool"
	optionTypeInt    = "int"
)

type typeOption struct {
	ttype optionType

	reader func(*snowflake.FileFormatOptions) interface{}
}

var fileFormatTypeOptions = map[string]map[string]typeOption{
	"csv": {
		"compression": {
			ttype:  optionTypeString,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.Compression },
		},
		"record_delimiter": {
			ttype:  optionTypeString,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.RecordDelimiter },
		},
		"field_delimiter": {
			ttype:  optionTypeString,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.FieldDelimiter },
		},
		"file_extension": {
			ttype:  optionTypeString,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.FileExtension },
		},
		"trim_space": {
			ttype:  optionTypeBool,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.TrimSpace },
		},
		"skip_header": {
			ttype:  optionTypeInt,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.SkipHeader },
		},
		"skip_blank_lines": {
			ttype:  optionTypeBool,
			reader: func(o *snowflake.FileFormatOptions) interface{} { return o.SkipBlankLines },
		},
	},
}

type fileFromatID struct {
	DatabaseName   string
	SchemaName     string
	FileFormatName string
}

// String() takes in a stageID object and returns a pipe-delimited string:
// DatabaseName|SchemaName|StageName
func (si *fileFromatID) String() (string, error) {
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)
	csvWriter.Comma = stageIDDelimiter
	dataIdentifiers := [][]string{{si.DatabaseName, si.SchemaName, si.FileFormatName}}
	err := csvWriter.WriteAll(dataIdentifiers)
	if err != nil {
		return "", err
	}
	strStageID := strings.TrimSpace(buf.String())
	return strStageID, nil
}

func FileFormat() *schema.Resource {
	return &schema.Resource{
		Create: CreateFileFormat,
		Read:   ReadFileFormat,
		Update: UpdateFileFormat,
		Delete: DeleteFileFormat,

		Schema: fileFormatSchema,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
	}
}

func fileFormatIDFromString(stringID string) (*fileFromatID, error) {
	reader := csv.NewReader(strings.NewReader(stringID))
	reader.Comma = '|'
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("Not CSV compatible")
	}

	if len(lines) != 1 {
		return nil, fmt.Errorf("1 line per stage")
	}
	if len(lines[0]) != 3 {
		return nil, fmt.Errorf("3 fields allowed")
	}

	fileFormat := &fileFromatID{
		DatabaseName:   lines[0][0],
		SchemaName:     lines[0][1],
		FileFormatName: lines[0][2],
	}
	return fileFormat, nil
}

func getTypeAndParams(d *schema.ResourceData) (string, map[string]interface{}, error) {
	types := []string{
		"csv",
		"json",
		"avro",
		"orc",
		"parquet",
		"xml",
	}

	for _, ttype := range types {
		if v, ok := d.GetOkExists(ttype); ok {
			t := v.(*schema.Set)
			log.Printf("[DEBUG] %#v", t)
			return ttype, t.List()[0].(map[string]interface{}), nil
		}
	}

	return "", nil, errors.New("could not extract file format parameters")
}

func CreateFileFormat(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Get("name").(string)
	database := d.Get("database").(string)
	schema := d.Get("schema").(string)

	ttype, params, err := getTypeAndParams(d)
	log.Printf("[DEBUG] params %#v", params)
	if err != nil {
		return err
	}

	builder := snowflake.FileFormat(database, schema, name).Create(ttype)

	for _, p := range fileFormatProperties {
		if v, ok := d.GetOk(p); ok {
			builder.SetString(p, v.(string))
		}
	}

	switch ttype {
	case "json":
		if v, ok := params["compression"]; ok && v != "" {
			builder.SetString("compression", v.(string))
		}
	case "csv":
		for _, options := range fileFormatTypeOptions {
			for name, opt := range options {
				if v, ok := params[name]; ok && v != "" {
					switch opt.ttype {
					case optionTypeString:
						builder.SetString(name, v.(string))
					}
				}
			}
		}
	}

	err = snowflake.Exec(db, builder.Statement())
	if err != nil {
		return errors.Wrap(err, "unable to create file format")
	}

	id := &fileFromatID{
		DatabaseName:   database,
		SchemaName:     schema,
		FileFormatName: name,
	}
	dataIDInput, err := id.String()
	if err != nil {
		return err
	}
	d.SetId(dataIDInput)

	return ReadFileFormat(d, meta)
}

func ReadFileFormat(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	stageID, err := stageIDFromString(d.Id())
	if err != nil {
		return err
	}

	dbName := stageID.DatabaseName
	schema := stageID.SchemaName
	stage := stageID.StageName

	q := snowflake.FileFormat(stage, dbName, schema).Show()
	row := snowflake.QueryRow(db, q)
	if row.Err() == sql.ErrNoRows {
		// If not found, mark resource to be removed from statefile during apply or refresh
		log.Printf("[DEBUG] file format (%s) not found", d.Id())
		d.SetId("")
		return nil
	}
	if row.Err() != nil {
		return err
	}

	ff, err := snowflake.ScanFileFormat(row)
	if err != nil {
		return errors.Wrap(err, "unable to scan file format row")
	}

	err = d.Set("database", ff.Database.String)
	if err != nil {
		return err
	}

	err = d.Set("schema", ff.Schema.String)
	if err != nil {
		return err
	}

	err = d.Set("name", ff.Name.String)
	if err != nil {
		return err
	}

	err = d.Set("comment", ff.Comment.String)
	if err != nil {
		return err
	}

	err = d.Set("type", ff.TType.String)
	if err != nil {
		return err
	}

	asdf := map[string]interface{}{}

	for n, opt := range fileFormatTypeOptions[strings.ToLower(ff.TType.String)] {
		if v := opt.reader(ff.ParsedFormatOptions); v != nil {
			asdf[n] = v
		}
	}

	a := []map[string]interface{}{asdf}

	log.Printf("[DEBUG] asdf %#v %#v", strings.ToLower(ff.TType.String), a)
	err = d.Set(strings.ToLower(ff.TType.String), a)
	if err != nil {
		return err
	}

	return nil
}

func UpdateFileFormat(d *schema.ResourceData, meta interface{}) error {
	return errors.New("not implemented")
}

func DeleteFileFormat(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	ffID, err := fileFormatIDFromString(d.Id())
	if err != nil {
		return errors.Wrapf(err, "unable to parse file format id %s", d.Id())
	}

	builder := snowflake.FileFormat(ffID.DatabaseName, ffID.SchemaName, ffID.FileFormatName)

	return snowflake.Exec(db, builder.Drop())
}
