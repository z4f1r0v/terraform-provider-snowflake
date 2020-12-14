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
	"database",
	"schema",
	"name",
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
	"type": {
		Type:     schema.TypeString,
		Required: true,
		// Description:  "",
		ValidateFunc: validation.StringInSlice([]string{"CSV", "JSON", "AVRO", "ORC", "PARQUET", "XML"}, true),
	},
	"comment": {
		Type:     schema.TypeString,
		Optional: true,
		// Description:  "",
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

func CreateFileFormat(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	name := d.Get("name").(string)
	database := d.Get("database").(string)
	schema := d.Get("schema").(string)

	ttype := d.Get("type").(string)

	builder := snowflake.FileFormat(database, schema, name).Create(ttype)

	for _, p := range fileFormatProperties {
		if v, ok := d.GetOk(p); ok {
			builder.SetString(p, v.(string))
		}
	}

	err := snowflake.Exec(db, builder.Statement())
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

	err = d.Set("database", ff.Database)
	if err != nil {
		return err
	}

	err = d.Set("schema", ff.Schema)
	if err != nil {
		return err
	}

	err = d.Set("name", ff.Name)
	if err != nil {
		return err
	}

	err = d.Set("type", ff.TType)
	if err != nil {
		return err
	}

	err = d.Set("comment", ff.Comment)
	if err != nil {
		return err
	}

	return nil
}

func UpdateFileFormat(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func DeleteFileFormat(d *schema.ResourceData, meta interface{}) error {
	return nil
}
