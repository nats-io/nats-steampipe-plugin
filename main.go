package main

import (
	"github.com/ConnectEverything/steampipe-plugin-nats/nats"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{PluginFunc: nats.Plugin})
}
