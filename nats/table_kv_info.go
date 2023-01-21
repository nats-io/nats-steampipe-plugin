package nats

import (
	"context"
	"fmt"
	"math"
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

			{Name: "last_updated_human", Type: proto.ColumnType_STRING, Transform: transform.FromField("LastUpdatedHuman")},

			{Name: "last_updated_seconds", Type: proto.ColumnType_INT, Transform: transform.FromField("LastUpdatedSeconds")},
		},
	}
}

type KV struct {
	Bucket      string `json:"bucket"`
	Description string `json:"description"`
	Created     string `json:"created"`
	Size        string `json:"size"`
	Values      int64  `json:"values"`
	// human readable for last updated
	LastUpdatedHuman string `json:"last_updated_human"`
	// machine readable number of seconds since last update
	LastUpdatedSeconds int64 `json:"last_updated_seconds"`
}

func formatUpdatedTime(t time.Time) string {
	diff := time.Since(t)

	if diff == math.MaxInt64 {
		return "never"
	}

	seconds := diff / time.Second
	minutes := seconds / 60
	hours := minutes / 60

	return fmt.Sprintf("%dh%dm%ds", hours%24, minutes%60, seconds%60)
}

func formatUnix(t time.Time) int64 {
	diff := time.Since(t)

	if diff == math.MaxInt64 {
		return 0
	}

	return int64(diff.Seconds())
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
			Bucket:             strings.TrimPrefix(k.Name(), "KV_"),
			Description:        k.Description(),
			Created:            info.Created.Format("2006-01-02 15:04:05"),
			Size:               humanize.IBytes(info.State.Bytes),
			Values:             int64(info.State.Msgs),
			LastUpdatedHuman:   formatUpdatedTime(info.State.LastTime),
			LastUpdatedSeconds: formatUnix(info.State.LastTime),
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

	return KV{
		Bucket:             strings.TrimPrefix(str.Name(), "KV_"),
		Description:        str.Description(),
		Created:            info.Created.Format("2006-01-02 15:04:05"),
		Size:               humanize.IBytes(info.State.Bytes),
		Values:             int64(info.State.Msgs),
		LastUpdatedHuman:   formatUpdatedTime(info.State.LastTime),
		LastUpdatedSeconds: formatUnix(info.State.LastTime),
	}, nil

}
