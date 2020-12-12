package resources

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var fileFormatSchema = map[string]*schema.Schema{}

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

func CreateFileFormat(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func ReadFileFormat(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func UpdateFileFormat(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func DeleteFileFormat(d *schema.ResourceData, meta interface{}) error {
	return nil
}
