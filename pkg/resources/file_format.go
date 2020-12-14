package resources

// import (
// 	"database/sql"
// 	"log"

// 	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
// 	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
// 	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
// )

// var fileFormatProperties = []string{}

// var fileFormatSchema = map[string]*schema.Schema{
// 	"database": {
// 		Type:        schema.TypeString,
// 		Required:    true,
// 		Description: "Name of the file format.",
// 	},
// 	"schema": {
// 		Type:        schema.TypeString,
// 		Required:    true,
// 		Description: "Name of the file format.",
// 	},
// 	"name": {
// 		Type:        schema.TypeString,
// 		Required:    true,
// 		Description: "Name of the file format.",
// 	},
// 	"type": {
// 		Type:     schema.TypeString,
// 		Required: true,
// 		// Description:  "",
// 		ValidateFunc: validation.StringInSlice([]string{"CSV", "JSON", "AVRO", "ORC", "PARQUET", "XML"}, true),
// 	},
// 	"comment": {
// 		Type:     schema.TypeString,
// 		Optional: true,
// 		// Description:  "",
// 	},
// }

// func FileFormat() *schema.Resource {
// 	return &schema.Resource{
// 		Create: CreateFileFormat,
// 		Read:   ReadFileFormat,
// 		Update: UpdateFileFormat,
// 		Delete: DeleteFileFormat,

// 		Schema: fileFormatSchema,
// 		Importer: &schema.ResourceImporter{
// 			StateContext: schema.ImportStatePassthroughContext,
// 		},
// 	}
// }

// func CreateFileFormat(d *schema.ResourceData, meta interface{}) error {
// 	return ReadFileFormat(d, meta)
// }

// func ReadFileFormat(d *schema.ResourceData, meta interface{}) error {
// 	db := meta.(*sql.DB)
// 	id := d.Id()

// 	stmt := snowflake.FileFormat(id).Show()
// 	row := snowflake.QueryRow(db, stmt)

// 	u, err := snowflake.ScanFileFormat(row)
// 	if err == sql.ErrNoRows {
// 		// If not found, mark resource to be removed from statefile during apply or refresh
// 		log.Printf("[DEBUG] fileFormat (%s) not found", d.Id())
// 		d.SetId("")
// 		return nil
// 	}
// 	if err != nil {
// 		return err
// 	}

// 	err = d.Set("name", u.Name.String)
// 	if err != nil {
// 		return err
// 	}
// 	err = d.Set("comment", u.Comment.String)
// 	if err != nil {
// 		return err
// 	}

// 	return err
// }

// func UpdateFileFormat(d *schema.ResourceData, meta interface{}) error {
// 	return UpdateResource("fileFormat", fileFormatProperties, fileFormatSchema, snowflake.FileFormat, ReadFileFormat)(d, meta)
// }

// func DeleteFileFormat(d *schema.ResourceData, meta interface{}) error {
// 	return DeleteResource("fileFormat", snowflake.FileFormat)(d, meta)
// }
