package nats

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
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
			{Name: "name", Type: proto.ColumnType_STRING, Transform: transform.FromField("Config.Name")},
			{Name: "msgs", Type: proto.ColumnType_INT, Transform: transform.FromField("State.Msgs")},
			{Name: "bytes", Type: proto.ColumnType_INT, Transform: transform.FromField("State.Bytes")},
			{Name: "first_seq", Type: proto.ColumnType_INT, Transform: transform.FromField("State.FirstSeq")},
			{Name: "first_time", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("State.FirstTime")},
			{Name: "last_seq", Type: proto.ColumnType_INT, Transform: transform.FromField("State.LastSeq")},
			{Name: "last_time", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("State.LastTime")},
			{Name: "num_deleted", Type: proto.ColumnType_INT, Transform: transform.FromField("State.NumDeleted")},
			{Name: "num_subjects", Type: proto.ColumnType_INT, Transform: transform.FromField("State.NumSubjects")},
			{Name: "subjects", Type: proto.ColumnType_JSON, Transform: transform.FromField("State.Subjects")},
			{Name: "consumers", Type: proto.ColumnType_INT, Transform: transform.FromField("State.Consumers")},
			{Name: "cluster", Type: proto.ColumnType_STRING, Transform: transform.FromField("Cluster.Name")},
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

	streams, err := manager.Streams(nil)
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

	nameQuals := d.EqualsQuals
	//name := d.KeyColumnQuals["name"].GetStringValue()
	name := nameQuals["name"].GetStringValue()

	stream, err := manager.LoadStream(name)
	if err != nil {
		return nil, err
	}

	return stream.LatestInformation()
}
