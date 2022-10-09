package nats

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func streamInfo() *plugin.Table {
	return &plugin.Table{
		Name:        "stream_info",
		Description: "The stream info",
		List: &plugin.ListConfig{
			Hydrate: listStreamInfos,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("name"),
			Hydrate:    getStreamInfo,
		},
		Columns: []*plugin.Column{
			{Name: "name", Type: proto.ColumnType_STRING, Description: "The stream name", Transform: transform.FromField("Config.Name")},
			{Name: "msgs", Type: proto.ColumnType_INT, Description: "The stream msgs", Transform: transform.FromField("State.Msgs")},
			{Name: "bytes", Type: proto.ColumnType_INT, Description: "The stream bytes", Transform: transform.FromField("State.Bytes")},
			{Name: "first_seq", Type: proto.ColumnType_INT, Description: "The stream first seq", Transform: transform.FromField("State.FirstSeq")},
			{Name: "first_time", Type: proto.ColumnType_TIMESTAMP, Description: "The stream first time", Transform: transform.FromField("State.FirstTime")},
			{Name: "last_seq", Type: proto.ColumnType_INT, Description: "The stream last seq", Transform: transform.FromField("State.LastSeq")},
			{Name: "last_time", Type: proto.ColumnType_TIMESTAMP, Description: "The stream last time", Transform: transform.FromField("State.LastTime")},
			{Name: "num_deleted", Type: proto.ColumnType_INT, Description: "The num deleted", Transform: transform.FromField("State.NumDeleted")},
			{Name: "num_subjects", Type: proto.ColumnType_INT, Description: "The number of stream subjects", Transform: transform.FromField("State.NumSubjects")},
			{Name: "subjects", Type: proto.ColumnType_JSON, Description: "The stream subjects", Transform: transform.FromField("State.Subjects")},
			{Name: "consumers", Type: proto.ColumnType_INT, Description: "The number of consumers", Transform: transform.FromField("State.Consumers")},
			{Name: "cluster", Type: proto.ColumnType_STRING, Description: "Cluster name", Transform: transform.FromField("Cluster.Name")},
		},
	}
}

func listStreamInfos(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
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

	streams, err := manager.Streams()
	if err != nil {
		return nil, err
	}

	for _, v := range streams {
		info, err := v.LatestInformation()
		if err != nil {
			return nil, err
		}
		d.StreamListItem(ctx, info)
	}

	return nil, nil

}

func getStreamInfo(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
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

	name := d.KeyColumnQuals["name"].GetStringValue()

	stream, err := manager.LoadStream(name)
	if err != nil {
		return nil, err
	}

	return stream.LatestInformation()
}
