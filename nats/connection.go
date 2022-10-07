package nats

import (
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/schema"
)

type natsConfig struct {
	URLs           string  `cty:"urls"`
	MonitoringURLs *string `cty:"monitoring_urls"`
}

func connConfig() interface{} {
	return &natsConfig{}
}

var configSchema = map[string]*schema.Attribute{
	"urls": {
		Type:     schema.TypeString,
		Required: true,
	},
	"monitoring_urls": {
		Type: schema.TypeString,
	},
}

func GetConfig(conn *plugin.Connection) (natsConfig, error) {
	config, ok := conn.Config.(natsConfig)
	if !ok {
		return natsConfig{}, fmt.Errorf("really bad")
	}

	return config, nil
}
