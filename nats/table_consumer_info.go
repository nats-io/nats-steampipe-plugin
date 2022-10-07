package nats

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func consumerInfo() *plugin.Table {
	return &plugin.Table{
		Name:        "consumer_info",
		Description: "The consumer info",
		List: &plugin.ListConfig{
			Hydrate: listConsumerInfos,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getConsumerInfo,
		},
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Description: "The consumer name", Transform: transform.FromField("Name")},
			{Name: "stream", Type: proto.ColumnType_STRING, Description: "The name of the consumer's stream", Transform: transform.FromField("Stream")},
			{Name: "created", Type: proto.ColumnType_TIMESTAMP, Description: "The created time", Transform: transform.FromField("Created")},
			{Name: "delivered_consumer_seq", Type: proto.ColumnType_INT, Description: "The delivered consumer seq", Transform: transform.FromField("Delivered.Consumer")},
			{Name: "delivered_stream_seq", Type: proto.ColumnType_INT, Description: "The delivered stream seq", Transform: transform.FromField("Delivered.Stream")},
			{Name: "delivered_last_active", Type: proto.ColumnType_TIMESTAMP, Description: "The last active time", Transform: transform.FromField("Delivered.Last")},
			{Name: "ack_floor_consumer_seq", Type: proto.ColumnType_INT, Description: "The ack floor consumer seq", Transform: transform.FromField("AckFloor.Consumer")},
			{Name: "ack_floor_stream_seq", Type: proto.ColumnType_INT, Description: "The ack floor stream seq", Transform: transform.FromField("AckFloor.Stream")},
			{Name: "ack_floor_last_active", Type: proto.ColumnType_TIMESTAMP, Description: "The ack floor last active time", Transform: transform.FromField("AckFloor.Last")},
			{Name: "num_ack_pending", Type: proto.ColumnType_INT, Description: "The number of ack pending", Transform: transform.FromField("NumAckPending")},
			{Name: "num_redelivered", Type: proto.ColumnType_INT, Description: "The number of redelivered", Transform: transform.FromField("NumRedelivered")},
			{Name: "num_pending", Type: proto.ColumnType_INT, Description: "The number pending", Transform: transform.FromField("NumPending")},
			{Name: "cluster_name", Type: proto.ColumnType_STRING, Description: "The cluster name", Transform: transform.FromField("Cluster.Name")},
			{Name: "push_bound", Type: proto.ColumnType_BOOL, Description: "Push bound", Transform: transform.FromField("PushBound")},
		},
	}
}

func listConsumerInfos(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	config, err := GetConfig(d.Connection)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(config.URLs)
	if err != nil {
		return nil, err
	}

	manager, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	var streams []string

	err = manager.EachStream(func(s *jsm.Stream) {
		streams = append(streams, s.Configuration().Name)
	})
	if err != nil {
		return nil, err
	}

	for _, s := range streams {
		consumers, err := manager.Consumers(s)
		if err != nil {
			return nil, err
		}

		for _, v := range consumers {
			info, err := v.LatestState()
			if err != nil {
				return nil, err
			}

			d.StreamListItem(ctx, info)
		}
	}

	return nil, nil

}

func getConsumerInfo(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	config, err := GetConfig(d.Connection)
	if err != nil {
		return nil, err
	}

	nc, err := nats.Connect(config.URLs)
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

	return stream.LatestInformation()
}
