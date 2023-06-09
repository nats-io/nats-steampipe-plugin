package nats

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func streamConfigs() *plugin.Table {
	return &plugin.Table{
		Name:        "stream_configs",
		Description: "The stream configurations",
		List: &plugin.ListConfig{
			Hydrate: listStreamConfigs,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getStreamConfig,
		},
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Transform: transform.FromField("Name")},
			{Name: "description", Type: proto.ColumnType_STRING, Transform: transform.FromField("Description")},
			{Name: "subjects", Type: proto.ColumnType_STRING, Transform: transform.FromField("Subjects")},
			{Name: "retention", Type: proto.ColumnType_STRING, Transform: transform.FromField("Retention")},
			{Name: "max_consumers", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxConsumers")},
			{Name: "max_msgs", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxMsgs")},
			{Name: "max_msgs_per_subject", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxMsgsPer")},
			{Name: "storage", Type: proto.ColumnType_STRING, Transform: transform.FromField("Storage")},
			{Name: "replicas", Type: proto.ColumnType_INT, Transform: transform.FromField("Replicas")},
			{Name: "max_bytes", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxBytes")},
			{Name: "max_age", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxAge")},
			{Name: "max_msg_size", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxMsgSize")},
			{Name: "discard", Type: proto.ColumnType_STRING, Transform: transform.FromField("Discard")},
			{Name: "no_ack", Type: proto.ColumnType_BOOL, Transform: transform.FromField("NoAck")},
			{Name: "template", Type: proto.ColumnType_STRING, Transform: transform.FromField("Template")},
			{Name: "duplicates", Type: proto.ColumnType_INT, Transform: transform.FromField("Duplicates")},
			{Name: "placement", Type: proto.ColumnType_STRING, Transform: transform.FromField("Placement")},
			{Name: "mirror", Type: proto.ColumnType_STRING, Transform: transform.FromField("Mirror")},
			{Name: "sources", Type: proto.ColumnType_STRING, Transform: transform.FromField("Sources")},
			{Name: "republish", Type: proto.ColumnType_INT, Transform: transform.FromField("Replicas")},
			{Name: "sealed", Type: proto.ColumnType_BOOL, Transform: transform.FromField("Sealed")},
			{Name: "deny_delete", Type: proto.ColumnType_BOOL, Transform: transform.FromField("DenyDelete")},
			{Name: "deny_purge", Type: proto.ColumnType_BOOL, Transform: transform.FromField("DenyPurge")},
			{Name: "rollup_allowed", Type: proto.ColumnType_BOOL, Transform: transform.FromField("RollupAllowed")},
			{Name: "allow_direct", Type: proto.ColumnType_BOOL, Transform: transform.FromField("AllowDirect")},
			{Name: "mirror_direct", Type: proto.ColumnType_BOOL, Transform: transform.FromField("MirrorDirect")},
		},
	}
}

func listStreamConfigs(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	config, err := GetConfig(d.Connection)
	if err != nil {
		return nil, err
	}

	nc, err := config.Connect()
	if err != nil {
		return nil, err
	}

	manager, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	err = manager.EachStream(nil, func(s *jsm.Stream) {
		d.StreamListItem(ctx, s.Configuration())
	})

	if err != nil {
		return nil, err
	}

	return nil, nil

}

func getStreamConfig(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	config, err := GetConfig(d.Connection)
	if err != nil {
		return nil, err
	}

	nc, err := config.Connect()
	if err != nil {
		return nil, err
	}

	manager, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	nameQuals := d.EqualsQuals
	//name := d.KeyColumnQuals["name"].GetStringValue()
	name := nameQuals["name"].GetStringValue()

	stream, err := manager.LoadStream(name)
	if err != nil {
		return nil, err
	}

	return stream.Configuration(), nil
}
