package nats

import (
	"fmt"

	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/schema"
)

func String(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type natsConfig struct {
	Context       *string `cty:"context"`
	URLs          *string `cty:"urls"`
	MonitoringURL *string `cty:"monitoring_url"`
}

func (c *natsConfig) Connect() (*nats.Conn, error) {
	// TODO: more connection options?
	var opts []nats.Option

	// No URLs are set, use context.
	if String(c.URLs) == "" {
		return natscontext.Connect(String(c.Context), opts...)
	}

	return nats.Connect(String(c.URLs), opts...)
}

func connConfig() interface{} {
	return &natsConfig{}
}

var configSchema = map[string]*schema.Attribute{
	"context": {
		Type: schema.TypeString,
	},
	"urls": {
		Type: schema.TypeString,
	},
	"monitoring_url": {
		Type: schema.TypeString,
	},
}

func GetConfig(conn *plugin.Connection) (*natsConfig, error) {
	config, ok := conn.Config.(natsConfig)
	if !ok {
		return nil, fmt.Errorf("really bad")
	}

	return &config, nil
}
