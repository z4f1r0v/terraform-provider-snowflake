package resources

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/pkg/errors"
)

var fileFormatFormatTypes = []string{
	"csv",
	"json",
	"avro",
	"orc",
	"parquet",
	"xml",
}

var fileFormatProperties = []string{
	"type",
	"comment",
}

func debugf(msg string, params ...interface{}) { //nolint
	fmt.Printf("[DEBUG] %#v %#v\n", msg, params)
}

var fileFormatSchema = map[string]*schema.Schema{
	"database": {
		Type:        schema.TypeString,
		Required:    true,
		ForceNew:    true,
		Description: "Name of the file format.",
	},
	"schema": {
		Type:        schema.TypeString,
		Required:    true,
		ForceNew:    true,
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
		ForceNew: true,
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
				"date_format": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"time_format": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"timestamp_format": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"binary_format": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringInSlice([]string{"HEX", "BASE64", "UTF8"}, true),
				},
				"escape": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringLenBetween(1, 1),
				},
				"escape_unenclosed_field": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringLenBetween(1, 1),
				},
				"field_optionally_enclosed_by": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringLenBetween(1, 1),
				},
				"error_on_column_count_mismatch": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"replace_invalid_characters": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"validate_utf8": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"empty_field_as_null": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"skip_byte_order_mark": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"encoding": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"null_if": {
					Type:     schema.TypeList,
					Optional: true,
					Computed: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
				},
			},
		},
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
				"date_format": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"time_format": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"timestamp_format": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"binary_format": {
					Type:         schema.TypeString,
					Optional:     true,
					Computed:     true,
					ValidateFunc: validation.StringInSlice([]string{"HEX", "BASE64", "UTF8"}, true),
				},
				"null_if": {
					Type:     schema.TypeList,
					Optional: true,
					Computed: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
				},
				"file_extension": {
					Type:     schema.TypeString,
					Optional: true,
					Computed: true,
				},
				"replace_invalid_characters": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"skip_byte_order_mark": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"enable_octal": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"allow_duplicate": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"strip_outer_array": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"strip_null_values": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"ignore_utf8_errors": {
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
				"null_if": {
					Type:     schema.TypeList,
					Optional: true,
					Computed: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
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
				"null_if": {
					Type:     schema.TypeList,
					Optional: true,
					Computed: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
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
					// = AUTO | LZO | SNAPPY | NONE
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"binary_as_text": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"null_if": {
					Type:     schema.TypeList,
					Optional: true,
					Computed: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
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
					// COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
				},
				"trim_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"ignore_utf8_errors": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"null_if": {
					Type:     schema.TypeList,
					Optional: true,
					Computed: true,
					Elem: &schema.Schema{
						Type: schema.TypeString,
					},
				},
				"skip_byte_order_mark": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"preserve_space": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"strip_outer_element": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"disable_snowflake_data": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
				"disable_auto_convert": {
					Type:     schema.TypeBool,
					Optional: true,
					Computed: true,
				},
			},
		},
	},
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

func getFormatType(d *schema.ResourceData) (string, error) {
	types := []string{
		"csv",
		"json",
		"avro",
		"orc",
		"parquet",
		"xml",
	}

	for _, ttype := range types {
		if v, ok := d.GetOkExists(ttype); ok && len(v.(*schema.Set).List()) > 0 { //nolint
			return ttype, nil
		}
	}

	return "", errors.New("could not extract file format parameters")
}

func CreateFileFormat(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Get("name").(string)
	database := d.Get("database").(string)
	schema := d.Get("schema").(string)

	ttype, err := getFormatType(d)
	if err != nil {
		return err
	}

	builder := snowflake.FileFormat(database, schema, name).Create(ttype)

	for _, p := range fileFormatProperties {
		if v, ok := d.GetOk(p); ok {
			builder.SetString(p, v.(string))
		}
	}

	for name, opt := range snowflake.FileFormatTypeOptions[ttype] {
		if v, ok := d.GetOkExists(fmt.Sprintf("%s.0.%s", ttype, name)); ok && v != "" { //nolint
			switch opt.Type {
			case snowflake.OptionTypeString:
				builder.SetString(name, v.(string))
			case snowflake.OptionTypeBool:
				builder.SetBool(name, v.(bool))
			case snowflake.OptionTypeStringSlice:
				builder.SetStringList(name, v.([]string))
			}
		}
	}

	err = snowflake.Exec(db, builder.Statement())
	if err != nil {
		return errors.Wrap(err, "unable to create file format")
	}

	id := &fileFormatID{
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
	ffID, err := fileFormatIDFromString(d.Id())
	if err != nil {
		return err
	}

	dbName := ffID.DatabaseName
	schema := ffID.SchemaName
	name := ffID.FileFormatName

	q := snowflake.FileFormat(dbName, schema, name).Show()
	row := snowflake.QueryRow(db, q)
	if row.Err() == sql.ErrNoRows {
		// If not found, mark resource to be removed from state during apply or refresh
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

	data := map[string]interface{}{}
	for n, opt := range snowflake.FileFormatTypeOptions[strings.ToLower(ff.TType.String)] {
		if v := opt.Reader(ff.ParsedFormatOptions); v != nil {
			data[n] = v
		}
	}

	err = d.Set(strings.ToLower(ff.TType.String), []map[string]interface{}{data})
	if err != nil {
		return err
	}

	return nil
}

func UpdateFileFormat(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	database := d.Get("database").(string)
	schemaName := d.Get("schema").(string)
	name := d.Get("name").(string)

	ffID, err := fileFormatIDFromString(d.Id())
	if err != nil {
		return err
	}

	changes := 0
	for _, t := range fileFormatFormatTypes {
		if d.HasChange(t) {
			changes += 1
		}

	}
	if changes > 1 {
		return errors.New("cannot change format type")
	}

	if d.HasChange("name") {
		o, n := d.GetChange("name")
		old := o.(string)
		new := n.(string)
		builder := snowflake.FileFormat(database, schemaName, old)

		err := snowflake.Exec(db, builder.Rename(new))
		if err != nil {
			return errors.Wrap(err, "unable to rename file format")
		}

		ffID.FileFormatName = new

		id, err := ffID.String()
		if err != nil {
			return errors.Wrap(err, "unable to generate new file format id")
		}
		d.SetId(id)
	}

	ttype, err := getFormatType(d)
	if err != nil {
		return err
	}

	if d.HasChange(ttype) {
		before, after := d.GetChange(ttype)
		b := before.(*schema.Set).List()[0].(map[string]interface{})
		a := after.(*schema.Set).List()[0].(map[string]interface{})

		hasChange := false
		alter := snowflake.FileFormat(database, schemaName, name).Alter()
		for name, opt := range snowflake.FileFormatTypeOptions[ttype] {

			switch opt.Type {
			case snowflake.OptionTypeString:
				if b[name].(string) != a[name].(string) {
					hasChange = true
					alter.SetString(name, a[name].(string))
				}
			case snowflake.OptionTypeBool:
				if b[name].(bool) != a[name].(bool) {
					hasChange = true

					alter.SetBool(name, a[name].(bool))
				}
			case snowflake.OptionTypeStringSlice:
				// TODO
				// if b[name].([]string) != a[name].([]string) {
				// 	debugf("has change")
				// 	hasChange = true

				// 	alter.SetStringList(name, a[name].([]string))
				// }
			}
		}
		if hasChange {
			err := snowflake.Exec(db, alter.Statement())
			if err != nil {
				return errors.Wrap(err, "error altering")
			}
		}
	}

	return ReadFileFormat(d, meta)
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
