package nats

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func Plugin(ctx context.Context) *plugin.Plugin {
	p := &plugin.Plugin{
		Name:             "steampipe-plugin-nats",
		DefaultTransform: transform.FromGo().NullIfZero(),
		ConnectionConfigSchema: &plugin.ConnectionConfigSchema{
			NewInstance: connConfig,
			Schema:      configSchema,
		},
		TableMap: map[string]*plugin.Table{
			"jetstream_streams": jetstreamStreams(),
		},
	}
	return p
}
