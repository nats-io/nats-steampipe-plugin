package nats

import (
	"fmt"

	"github.com/nats-io/jsm.go/natscontext"
	"github.com/nats-io/nats.go"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/schema"
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
	Creds         *string `cty:"creds"`
	Nkey          *string `cty:"nkey"`
	Username      *string `cty:"username"`
	Password      *string `cty:"password"`
	TLSCert       *string `cty:"tlscert"`
	TLSKey        *string `cty:"tlskey"`
	TLSCACert     *string `cty:"tlscacert"`
}

func (c *natsConfig) Connect() (*nats.Conn, error) {
	opts, err := c.getOptions()
	if err != nil {
		return nil, err
	}

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
	"creds": {
		Type: schema.TypeString,
	},
	"nkey": {
		Type: schema.TypeString,
	},
	"username": {
		Type: schema.TypeString,
	},
	"password": {
		Type: schema.TypeString,
	},
	"tlscert": {
		Type: schema.TypeString,
	},
	"tlskey": {
		Type: schema.TypeString,
	},
	"tlscacert": {
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

func (c *natsConfig) getOptions() ([]nats.Option, error) {
	var opts []nats.Option
	if c.Creds != nil {
		opts = append(opts, nats.UserCredentials(*c.Creds))
	}
	if c.Username != nil && c.Password != nil {
		opts = append(opts, nats.UserInfo(*c.Username, *c.Password))
	}
	if c.TLSCert != nil && c.TLSKey != nil {
		opts = append(opts, nats.ClientCert(*c.TLSCert, *c.TLSKey))
	}
	if c.TLSCACert != nil {
		opts = append(opts, nats.RootCAs(*c.TLSCACert))
	}
	if c.Nkey != nil {
		nkey, err := nats.NkeyOptionFromSeed(*c.Nkey)
		if err != nil {
			return nil, err
		}

		opts = append(opts, nkey)
	}

	return opts, nil
}
