package main

import (
	"github.com/hooksie1/natspipe/nats"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{PluginFunc: nats.Plugin})
}