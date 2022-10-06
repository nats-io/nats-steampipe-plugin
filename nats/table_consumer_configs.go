package nats

import (
	"context"

	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v4/plugin/transform"
)

func consumerConfigs() *plugin.Table {
	return &plugin.Table{
		Name:        "consumer_configs",
		Description: "The consumer configurations",
		List: &plugin.ListConfig{
			Hydrate: listConsumerConfigs,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"name", "stream"}),
			Hydrate:    getConsumerConfig,
		},
		Columns: []*plugin.Column{
			{Name: "stream", Type: proto.ColumnType_STRING, Description: "The stream name", Transform: transform.FromField("Stream")},
			{Name: "name", Type: proto.ColumnType_STRING, Description: "The consumer name", Transform: transform.FromField("Config.Name")},
			{Name: "filter_subject", Type: proto.ColumnType_STRING, Description: "The filter subject", Transform: transform.FromField("Config.FilterSubject")},
			{Name: "description", Type: proto.ColumnType_STRING, Description: "The consumer description", Transform: transform.FromField("Config.Description")},
			{Name: "ack_policy", Type: proto.ColumnType_STRING, Description: "The consumer ack policy", Transform: transform.FromField("Config.AckPolicy")},
			{Name: "ack_wait", Type: proto.ColumnType_INT, Description: "The ack wait duration", Transform: transform.FromField("Config.AckWait")},
			{Name: "deliver_policy", Type: proto.ColumnType_STRING, Description: "The consumer deliver policy", Transform: transform.FromField("Config.DeliverPolicy")},
			{Name: "deliver_subject", Type: proto.ColumnType_STRING, Description: "The consumer deliver subject", Transform: transform.FromField("Config.DeliverSubject")},
			{Name: "deliver_group", Type: proto.ColumnType_STRING, Description: "The consumer deliver group", Transform: transform.FromField("Config.DeliverGroup")},
			{Name: "durable", Type: proto.ColumnType_STRING, Description: "The consumer durable name", Transform: transform.FromField("Config.Durable")},
			{Name: "flow_control", Type: proto.ColumnType_BOOL, Description: "The consumer flow_control", Transform: transform.FromField("Config.FlowControl")},
			{Name: "heart_beat", Type: proto.ColumnType_INT, Description: "The consumer hearbeat duration", Transform: transform.FromField("Config.Heartbeat")},
			{Name: "max_ack_pending", Type: proto.ColumnType_INT, Description: "The consumer max ack pending", Transform: transform.FromField("Config.MaxAckPending")},
			{Name: "max_deliver", Type: proto.ColumnType_INT, Description: "The consumer max deliver", Transform: transform.FromField("Config.MaxDeliver")},
			{Name: "max_waiting", Type: proto.ColumnType_INT, Description: "The consumer max waiting", Transform: transform.FromField("Config.MaxWaiting")},
			{Name: "opt_start_seq", Type: proto.ColumnType_INT, Description: "The consumer start seq", Transform: transform.FromField("Config.OptStartSeq")},
			{Name: "opt_start_time", Type: proto.ColumnType_TIMESTAMP, Description: "The consumer opt start time", Transform: transform.FromField("Config.OptStartTime")},
			{Name: "rate_limit", Type: proto.ColumnType_INT, Description: "The consumer rate limit", Transform: transform.FromField("Config.RateLimit")},
			{Name: "replay_policy", Type: proto.ColumnType_STRING, Description: "The consumer replay policy", Transform: transform.FromField("Config.ReplayPolicy")},
			{Name: "sample_frequency", Type: proto.ColumnType_STRING, Description: "The consumer sample frequency", Transform: transform.FromField("Config.SampleFrequency")},
			{Name: "headers_only", Type: proto.ColumnType_BOOL, Description: "Headers only", Transform: transform.FromField("Config.HeadersOnly")},
			{Name: "max_batch", Type: proto.ColumnType_INT, Description: "The consumer max batch", Transform: transform.FromField("Config.MaxRequestBatch")},
			{Name: "max_expires", Type: proto.ColumnType_INT, Description: "The consumer max request expiry", Transform: transform.FromField("Config.MaxRequestExpires")},
			{Name: "max_bytes", Type: proto.ColumnType_INT, Description: "The consumer max bytes", Transform: transform.FromField("Config.MaxRequestMaxBytes")},
			{Name: "inactive_threshold", Type: proto.ColumnType_INT, Description: "The consumer inactive threshold", Transform: transform.FromField("Config.InactiveThreshold")},
			{Name: "replicas", Type: proto.ColumnType_INT, Description: "The consumer replicas", Transform: transform.FromField("Config.Replicas")},
			{Name: "mem_storage", Type: proto.ColumnType_BOOL, Description: "Memory storage", Transform: transform.FromField("Config.MemoryStorage")},
			{Name: "direct", Type: proto.ColumnType_BOOL, Description: "Direct", Transform: transform.FromField("Config.Direct")},
		},
	}
}

type ConsumerConfig struct {
	Stream string `json:"stream"`
	Config *api.ConsumerConfig
}

func listConsumerConfigs(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	urls := d.KeyColumnQualString("urls")
	nc, err := nats.Connect(urls)
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
			cfg := v.Configuration()
			c := ConsumerConfig{
				Stream: s,
				Config: &cfg,
			}

			d.StreamListItem(ctx, c)
		}

	}

	return nil, nil

}

func getConsumerConfig(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	urls := d.KeyColumnQualString("urls")
	nc, err := nats.Connect(urls)
	if err != nil {
		return nil, err
	}

	manager, err := jsm.New(nc)
	if err != nil {
		return nil, err
	}

	stream := d.KeyColumnQuals["stream"].GetStringValue()
	name := d.KeyColumnQuals["name"].GetStringValue()

	consumer, err := manager.LoadConsumer(stream, name)
	if err != nil {
		return nil, err
	}

	cfg := consumer.Configuration()

	c := ConsumerConfig{
		Stream: stream,
		Config: &cfg,
	}

	return c, nil
}
