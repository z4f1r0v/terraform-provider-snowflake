package snowflake_test

import (
	"testing"

	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
)

func builder(r *require.Assertions) *snowflake.FileFormatBuilder {
	ff := snowflake.FileFormat("db", "schema", "ff")
	r.NotNil(ff)
	return ff
}

func TestFileFormatShow(t *testing.T) {
	r := require.New(t)

	ff := builder(r)

	r.Equal(`SHOW FILE FORMATS LIKE 'ff' in SCHEMA db.schema`, ff.Show())
}

func TestFileFormatCreate(t *testing.T) {
	r := require.New(t)

	f := builder(r)

	createCSV := f.Create("CSV")
	r.Equal(`CREATE FILE FORMAT "db"."schema"."ff" TYPE='CSV'`, createCSV.Statement())

	createCSV.SetString("COMPRESSION", "GZIP")
	r.Equal(`CREATE FILE FORMAT "db"."schema"."ff" COMPRESSION='GZIP' TYPE='CSV'`, createCSV.Statement())
}

func TestFileFormatRename(t *testing.T) {
	r := require.New(t)

	f := builder(r)

	r.Equal(`ALTER FILE FORMAT "db"."schema"."ff" RENAME TO "db"."schema"."foo"`, f.Rename("foo"))
}

func TestFileFormatAlter(t *testing.T) {
	r := require.New(t)

	f := builder(r)

	a := f.Alter()
	a.SetString("COMPRESSION", "gzip")
	r.Equal(`ALTER FILE FORMAT "db"."schema"."ff" SET COMPRESSION='gzip'`, a.Statement())
}
