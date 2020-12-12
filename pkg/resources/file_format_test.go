package resources_test

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/provider"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/resources"
	. "github.com/chanzuckerberg/terraform-provider-snowflake/pkg/testhelpers"
	"github.com/stretchr/testify/require"
)

func TestFileFormat(t *testing.T) {
	r := require.New(t)
	err := resources.FileFormat().InternalValidate(provider.Provider().Schema, true)
	r.NoError(err)
}

// func TestUserCreate(t *testing.T) {
// 	r := require.New(t)

// 	in := map[string]interface{}{
// 		"name":                 "good_name",
// 		"comment":              "great comment",
// 		"password":             "awesomepassword",
// 		"login_name":           "gname",
// 		"display_name":         "Display Name",
// 		"first_name":           "Marcin",
// 		"last_name":            "Zukowski",
// 		"email":                "fake@email.com",
// 		"disabled":             true,
// 		"default_warehouse":    "mywarehouse",
// 		"default_namespace":    "mynamespace",
// 		"default_role":         "bestrole",
// 		"rsa_public_key":       "asdf",
// 		"rsa_public_key_2":     "asdf2",
// 		"must_change_password": true,
// 	}
// 	d := schema.TestResourceDataRaw(t, resources.User().Schema, in)
// 	r.NotNil(d)

// 	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
// 		mock.ExpectExec(`^CREATE USER "good_name" COMMENT='great comment' DEFAULT_NAMESPACE='mynamespace' DEFAULT_ROLE='bestrole' DEFAULT_WAREHOUSE='mywarehouse' DISPLAY_NAME='Display Name' EMAIL='fake@email.com' FIRST_NAME='Marcin' LAST_NAME='Zukowski' LOGIN_NAME='gname' PASSWORD='awesomepassword' RSA_PUBLIC_KEY='asdf' RSA_PUBLIC_KEY_2='asdf2' DISABLED=true MUST_CHANGE_PASSWORD=true$`).WillReturnResult(sqlmock.NewResult(1, 1))
// 		expectReadUser(mock)
// 		err := resources.CreateUser(d, db)
// 		r.NoError(err)
// 	})
// }

func expectReadFileFormat(mock sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{
		"created_on", "name", "database_name", "schema_name", "type", "owner", "comment", "format_options"},
	).AddRow("created_on", "file_format", "database", "schema", "csv", "asdf", "", "")
	mock.ExpectQuery(`^SHOW FILE FORMATS LIKE 'file_format'$`).WillReturnRows(rows)
}

func TestFileFormat_read(t *testing.T) {
	r := require.New(t)

	d := fileFormat(t, "file_format", map[string]interface{}{
		"database": "db",
		"schema":   "schema",
		"name":     "file_format",
	})

	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
		expectReadFileFormat(mock)
		err := resources.ReadFileFormat(d, db)
		r.NoError(err)
		// r.Equal("mock comment", d.Get("comment").(string))
		// r.Equal("myloginname", d.Get("login_name").(string))
		// r.Equal(false, d.Get("disabled").(bool))

		// Test when resource is not found, checking if state will be empty
		// r.NotEmpty(d.State())
		// q := snowflake.User(d.Id()).Show()
		// mock.ExpectQuery(q).WillReturnError(sql.ErrNoRows)
		// err2 := resources.ReadUser(d, db)
		// r.Empty(d.State())
		// r.Nil(err2)
	})
}

// func TestUserExists(t *testing.T) {
// 	r := require.New(t)

// 	d := user(t, "good_name", map[string]interface{}{"name": "good_name"})

// 	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
// 		expectReadUser(mock)
// 		b, err := resources.UserExists(d, db)
// 		r.NoError(err)
// 		r.True(b)
// 	})
// }

// func TestUserDelete(t *testing.T) {
// 	r := require.New(t)

// 	d := user(t, "drop_it", map[string]interface{}{"name": "drop_it"})

// 	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
// 		mock.ExpectExec(`^DROP USER "drop_it"$`).WillReturnResult(sqlmock.NewResult(1, 1))
// 		err := resources.DeleteUser(d, db)
// 		r.NoError(err)
// 	})
// }
