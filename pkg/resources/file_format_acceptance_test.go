package resources_test

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// TODO test for errors handling when trying to change type
func TestAccFileFormat_empty(t *testing.T) {
	types := map[string]map[string]string{
		"csv": {
			"compression": "AUTO",
			"trim_space":  "false",
		},
		"json": {
			"compression": "AUTO",
			"trim_space":  "false",
		},
		"avro": {
			"compression": "AUTO",
			"trim_space":  "false",
		},
		"orc": {
			"trim_space": "false",
		},
		"parquet": {
			"compression": "AUTO",
			"trim_space":  "false",
		},
		"xml": {
			"compression": "AUTO",
			"trim_space":  "false",
		},
	}

	for ttype, params := range types {
		t.Run(ttype, func(t *testing.T) {
			name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

			checks := []resource.TestCheckFunc{}

			for k, v := range params {
				checks = append(checks, resource.TestCheckResourceAttr("snowflake_file_format.ff", fmt.Sprintf("%s.0.%s", ttype, k), v))
			}
			resource.ParallelTest(t, resource.TestCase{
				Providers: providers(),
				Steps: []resource.TestStep{
					{
						Config: ffConfig(name, ttype),
						Check: resource.ComposeTestCheckFunc(
							resource.TestCheckResourceAttr("snowflake_database.d", "name", name),
							resource.TestCheckResourceAttr("snowflake_schema.s", "name", name),
							resource.TestCheckResourceAttr("snowflake_file_format.ff", "type", strings.ToUpper(ttype)),
							resource.ComposeTestCheckFunc(checks...),
						),
					},
					// RENAME
					// CHANGE PROPERTIES
					// IMPORT
				},
			})
		})
	}
}

func ffConfig(name, ttype string) string {
	s := `
resource snowflake_database d {
	name = "%s"
}

resource snowflake_schema s {
	database = snowflake_database.d.name
	name = "%s"
}

resource snowflake_file_format ff {
	database = snowflake_database.d.name
	schema = snowflake_schema.s.name
	name = "%s"
	
	%s {}
}
`
	s = fmt.Sprintf(s, name, name, name, ttype)
	log.Println(s)
	return s
}
