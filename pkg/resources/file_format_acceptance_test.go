package resources_test

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccFileFormat(t *testing.T) {
	// r := require.New(t)
	resource.ParallelTest(t, resource.TestCase{
		Providers: providers(),
		Steps:     []resource.TestStep{
			// {
			// 	Config: uConfig(prefix, sshkey1, sshkey2),
			// 	Check: resource.ComposeTestCheckFunc(
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "name", prefix),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "comment", "test comment"),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "login_name", strings.ToUpper(fmt.Sprintf("%s_login", prefix))),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "display_name", "Display Name"),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "first_name", "Marcin"),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "last_name", "Zukowski"),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "email", "fake@email.com"),
			// 		checkBool("snowflake_user.w", "disabled", false),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "default_warehouse", "foo"),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "default_role", "foo"),
			// 		resource.TestCheckResourceAttr("snowflake_user.w", "default_namespace", "FOO"),
			// 		checkBool("snowflake_user.w", "has_rsa_public_key", true),
			// 		checkBool("snowflake_user.w", "must_change_password", true),
			// 	),
			// },
			// IMPORT
			// {
			// 	ResourceName:            "snowflake_user.w",
			// 	ImportState:             true,
			// 	ImportStateVerify:       true,
			// 	ImportStateVerifyIgnore: []string{"password", "rsa_public_key", "rsa_public_key_2", "must_change_password"},
			// },
		},
	})
}

// func uConfig(prefix, key1, key2 string) string {
// 	s := `
// resource "snowflake_user" "w" {
// 	name = "%s"
// 	comment = "test comment"
// 	login_name = "%s_login"
// 	display_name = "Display Name"
// 	first_name = "Marcin"
// 	last_name = "Zukowski"
// 	email = "fake@email.com"
// 	disabled = false
// 	default_warehouse="foo"
// 	default_role="foo"
// 	default_namespace="foo"
// 	rsa_public_key = <<KEY
// %s
// KEY
// 	rsa_public_key_2 = <<KEY
// %s
// KEY
// 	must_change_password = true
// }
// `
// 	s = fmt.Sprintf(s, prefix, prefix, key1, key2)
// 	log.Printf("[DEBUG] s %s", s)
// 	return s
// }
