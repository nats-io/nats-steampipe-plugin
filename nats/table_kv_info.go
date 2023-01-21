package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
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
			{Name: "created", Type: proto.ColumnType_STRING, Transform: transform.FromField("Created")},
			{Name: "size", Type: proto.ColumnType_STRING, Transform: transform.FromField("Size")},
			{Name: "values", Type: proto.ColumnType_INT, Transform: transform.FromField("Values")},
			{Name: "last_updated", Type: proto.ColumnType_STRING, Transform: transform.FromField("LastUpdated")},
		},
	}
}

type KV struct {
	Bucket      string `json:"bucket"`
	Description string `json:"description"`
	Created     string `json:"created"`
	Size        string `json:"size"`
	Values      int64  `json:"values"`
	LastUpdated string `json:"last_updated"`
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

		d.StreamListItem(ctx, KV{
			Bucket:      strings.TrimPrefix(k.Name(), "KV_"),
			Description: k.Description(),
			Created:     info.Created.Format("2006-01-02 15:04:05"),
			Size:        humanize.IBytes(info.State.Bytes),
			Values:      int64(info.State.Msgs),
			LastUpdated: time.Since(info.State.LastTime).String(),
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

	str, err := manager.LoadStream(fmt.Sprintf("KV_%s", name))
	if err != nil {
		return nil, err
	}

	info, err := str.LatestInformation()
	if err != nil {
		return nil, err
	}

	return KV{
		Bucket:      strings.TrimPrefix(str.Name(), "KV_"),
		Description: str.Description(),
		Created:     info.Created.Format("2006-01-02 15:04:05"),
		Size:        humanize.IBytes(info.State.Bytes),
		Values:      int64(info.State.Msgs),
		LastUpdated: time.Since(info.State.LastTime).String(),
	}, nil

}
