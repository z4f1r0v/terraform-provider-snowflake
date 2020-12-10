package resources_test

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccSchema_basic(t *testing.T) {
	accName := acctest.RandStringFromCharSet(10, acctest.CharSetAlpha)

	resource.ParallelTest(t, resource.TestCase{
		Providers: providers(),
		Steps: []resource.TestStep{
			{
				Config: schemaConfig(accName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_schema.test", "name", accName),
					resource.TestCheckResourceAttr("snowflake_schema.test", "database", accName),
					resource.TestCheckResourceAttr("snowflake_schema.test", "comment", "Terraform acceptance test"),
					checkBool("snowflake_schema.test", "is_transient", false),
					checkBool("snowflake_schema.test", "is_managed", false),
				),
			},
		},
	})
}

func TestAccSchema_zero_retention(t *testing.T) {
	accName := acctest.RandStringFromCharSet(10, acctest.CharSetAlpha)

	resource.ParallelTest(t, resource.TestCase{
		Providers: providers(),
		Steps: []resource.TestStep{
			{
				Config: schemaConfigRetention(accName, 0),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_schema.test", "name", accName),
					resource.TestCheckResourceAttr("snowflake_schema.test", "database", accName),
					resource.TestCheckResourceAttr("snowflake_schema.test", "comment", "Terraform acceptance test"),
					checkBool("snowflake_schema.test", "is_transient", false),
					checkBool("snowflake_schema.test", "is_managed", false),
				),
			},
		},
	})
}

func schemaConfig(n string) string {
	return fmt.Sprintf(`
resource "snowflake_database" "test" {
	name = "%v"
	comment = "Terraform acceptance test"
}

resource "snowflake_schema" "test" {
	name = "%v"
	database = snowflake_database.test.name
	comment = "Terraform acceptance test"
}
`, n, n)
}

func schemaConfigRetention(n string, retention int) string {
	return fmt.Sprintf(`
resource "snowflake_database" "test" {
	name = "%v"
	comment = "Terraform acceptance test"
}

resource "snowflake_schema" "test" {
	name = "%v"
	database = snowflake_database.test.name
	comment = "Terraform acceptance test"
	data_retention_days = %d
}
`, n, n, retention)
}
