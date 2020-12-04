package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/chanzuckerberg/go-misc/ver"
	"github.com/chanzuckerberg/terraform-provider-snowflake/pkg/provider"
	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	version := flag.Bool("version", false, "spit out version for resources here")
	flag.Parse()

	spew.Dump(provider.Provider().Schema)
	os.Exit(0)

	if *version {
		verString, err := ver.VersionStr()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(verString)
		return
	}

	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: provider.Provider,
	})
}
