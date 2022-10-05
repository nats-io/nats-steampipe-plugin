package nats

import "github.com/turbot/steampipe-plugin-sdk/v4/plugin/schema"

type natsConfig struct {
	URLs string `cty:"urls"`
}

func connConfig() interface{} {
	return &natsConfig{}
}

var configSchema = map[string]*schema.Attribute{
	"urls": {
		Type:     schema.TypeString,
		Required: true,
	},
}
