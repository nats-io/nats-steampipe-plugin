package nats

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func jetstreamStreams() *plugin.Table {
	return &plugin.Table{
		Name:        "jetstream_streams",
		Description: "The streams",
		List: &plugin.ListConfig{
			Hydrate: listStreams,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getStream,
		},
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Description: "The stream name", Transform: transform.FromField("Name")},
			{Name: "description", Type: proto.ColumnType_STRING, Description: "The stream description", Transform: transform.FromField("Description")},
			{Name: "subjects", Type: proto.ColumnType_STRING, Description: "The stream subjects", Transform: transform.FromField("Subjects")},
			{Name: "retention", Type: proto.ColumnType_STRING, Description: "The stream retention policy", Transform: transform.FromField("Retention")},
			{Name: "max_consumers", Type: proto.ColumnType_INT, Description: "The max number of consumers", Transform: transform.FromField("MaxConsumers")},
			{Name: "max_msgs", Type: proto.ColumnType_INT, Description: "The max number of messages", Transform: transform.FromField("MaxMsgs")},
			{Name: "storage", Type: proto.ColumnType_STRING, Description: "The stream storage", Transform: transform.FromField("Storage")},
			{Name: "replicas", Type: proto.ColumnType_INT, Description: "The stream replicas", Transform: transform.FromField("Replicas")},
		},
	}
}

func listStreams(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	urls := d.KeyColumnQualString("urls")
	nc, err := nats.Connect(urls)
	if err != nil {
		return nil, err
	}

	manager, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	err = manager.EachStream(func(s *jsm.Stream) {
		d.StreamListItem(ctx, s.Configuration())

	})

	return nil, nil

}

func getStream(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	urls := d.KeyColumnQualString("urls")
	nc, err := nats.Connect(urls)
	if err != nil {
		return nil, err
	}

	manager, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	name := d.KeyColumnQuals["name"].GetStringValue()

	stream, err := manager.LoadStream(name)
	if err != nil {
		return nil, err
	}

	return stream.Configuration(), nil
}
