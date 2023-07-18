package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/jsm.go"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func kvInfo() *plugin.Table {
	return &plugin.Table{
		Name:        "kv_info",
		Description: "KV info for a bucket",
		List: &plugin.ListConfig{
			Hydrate: listKVInfos,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("bucket"),
			Hydrate:    getKVInfo,
		},
		Columns: []*plugin.Column{
			{Name: "bucket", Type: proto.ColumnType_STRING, Transform: transform.FromField("Bucket")},
			{Name: "description", Type: proto.ColumnType_STRING, Transform: transform.FromField("Description")},
			{Name: "created", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("Created")},
			{Name: "total_bytes", Type: proto.ColumnType_INT, Transform: transform.FromField("TotalBytes")},
			{Name: "values", Type: proto.ColumnType_INT, Transform: transform.FromField("NumValues")},
			{Name: "updated", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("Updated")},
		},
	}
}

type KV struct {
	Bucket      string    `json:"bucket"`
	Description string    `json:"description"`
	Created     time.Time `json:"created"`
	TotalBytes  uint64    `json:"total_bytes"`
	NumValues   int64     `json:"num_values"`
	Updated     time.Time `json:"updated"`
}

func formatUpdatedTime(created, updated time.Time) time.Time {
	y, _, _ := updated.Date()
	// not sure why year == 1 here but that's what the empty bucket evaluates to
	if y == 1 {
		return created
	}

	return updated
}

func listKVInfos(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
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

	var streams []*jsm.Stream

	err = manager.EachStream(func(s *jsm.Stream) {
		if s.IsKVBucket() {
			streams = append(streams, s)
		}
	})
	if err != nil {
		return nil, err
	}

	for _, k := range streams {
		info, err := k.LatestInformation()
		if err != nil {
			return nil, err
		}

		updated := formatUpdatedTime(info.Created, info.State.LastTime)

		d.StreamListItem(ctx, KV{
			Bucket:      strings.TrimPrefix(k.Name(), "KV_"),
			Description: k.Description(),
			Created:     info.Created,
			TotalBytes:  info.State.Bytes,
			NumValues:   int64(info.State.Msgs),
			Updated:     updated,
		})

	}

	return nil, nil
}

func getKVInfo(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
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

	name := d.KeyColumnQuals["bucket"].GetStringValue()

	if !strings.HasPrefix("KV_", name) {
		name = fmt.Sprintf("KV_%s", name)
	}

	str, err := manager.LoadStream(name)
	if err != nil {
		return nil, err
	}

	info, err := str.LatestInformation()
	if err != nil {
		return nil, err
	}

	updated := formatUpdatedTime(info.Created, info.State.LastTime)

	return KV{
		Bucket:      strings.TrimPrefix(str.Name(), "KV_"),
		Description: str.Description(),
		Created:     info.Created,
		TotalBytes:  info.State.Bytes,
		NumValues:   int64(info.State.Msgs),
		Updated:     updated,
	}, nil

}
