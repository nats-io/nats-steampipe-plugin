package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

func varzInfo() *plugin.Table {
	return &plugin.Table{
		Name:        "varz_info",
		Description: "The varz info",
		List: &plugin.ListConfig{
			Hydrate: listVarzInfos,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("server_name"),
			Hydrate:    getVarzInfo,
		},
		Columns: []*plugin.Column{
			{Name: "server_id", Type: proto.ColumnType_STRING, Transform: transform.FromField("ID")},
			{Name: "server_name", Type: proto.ColumnType_STRING, Transform: transform.FromField("Name")},
			{Name: "version", Type: proto.ColumnType_STRING, Transform: transform.FromField("Version")},
			{Name: "proto", Type: proto.ColumnType_INT, Transform: transform.FromField("Proto")},
			{Name: "git_commit", Type: proto.ColumnType_STRING, Transform: transform.FromField("GitCommit")},
			{Name: "go", Type: proto.ColumnType_STRING, Transform: transform.FromField("GoVersion")},
			{Name: "host", Type: proto.ColumnType_STRING, Transform: transform.FromField("Host")},
			{Name: "port", Type: proto.ColumnType_INT, Transform: transform.FromField("Port")},
			{Name: "auth_required", Type: proto.ColumnType_BOOL, Transform: transform.FromField("AuthRequired")},
			{Name: "tls_required", Type: proto.ColumnType_BOOL, Transform: transform.FromField("TLSRequired")},
			{Name: "tls_verify", Type: proto.ColumnType_BOOL, Transform: transform.FromField("TLSVerify")},
			{Name: "ip", Type: proto.ColumnType_STRING, Transform: transform.FromField("IP")},
			{Name: "connect_urls", Type: proto.ColumnType_STRING, Transform: transform.FromField("ClientConnectURLs")},
			{Name: "ws_connect_urls", Type: proto.ColumnType_STRING, Transform: transform.FromField("WSConnectURLs")},
			{Name: "max_connections", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxConn")},
			{Name: "max_subscriptions", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxSubs")},
			{Name: "ping_interval", Type: proto.ColumnType_INT, Transform: transform.FromField("PingInterval")},
			{Name: "ping_max", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxPingsOut")},
			{Name: "http_host", Type: proto.ColumnType_STRING, Transform: transform.FromField("HTTPHost")},
			{Name: "http_port", Type: proto.ColumnType_INT, Transform: transform.FromField("HTTPPort")},
			{Name: "http_base_path", Type: proto.ColumnType_STRING, Transform: transform.FromField("HTTPBasePath")},
			{Name: "https_port", Type: proto.ColumnType_INT, Transform: transform.FromField("HTTPSPort")},
			{Name: "auth_timeout", Type: proto.ColumnType_DOUBLE, Transform: transform.FromField("AuthTimeout")},
			{Name: "max_control_line", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxControlLine")},
			{Name: "max_payload", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxPayload")},
			{Name: "max_pending", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxPending")},
			{Name: "tls_timeout", Type: proto.ColumnType_DOUBLE, Transform: transform.FromField("TLSTimeout")},
			{Name: "write_deadline", Type: proto.ColumnType_INT, Transform: transform.FromField("WriteDeadline")},
			{Name: "start", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("Start")},
			{Name: "now", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("Now")},
			{Name: "uptime", Type: proto.ColumnType_STRING, Transform: transform.FromField("Uptime")},
			{Name: "mem", Type: proto.ColumnType_INT, Transform: transform.FromField("Mem")},
			{Name: "cores", Type: proto.ColumnType_INT, Transform: transform.FromField("Cores")},
			{Name: "gomaxprocs", Type: proto.ColumnType_INT, Transform: transform.FromField("MaxProcs")},
			{Name: "cpu", Type: proto.ColumnType_DOUBLE, Transform: transform.FromField("CPU")},
			{Name: "connections", Type: proto.ColumnType_INT, Transform: transform.FromField("Connections")},
			{Name: "total_connections", Type: proto.ColumnType_INT, Transform: transform.FromField("TotalConnections")},
			{Name: "routes", Type: proto.ColumnType_INT, Transform: transform.FromField("Routes")},
			{Name: "remotes", Type: proto.ColumnType_INT, Transform: transform.FromField("Remotes")},
			{Name: "leafnodes", Type: proto.ColumnType_INT, Transform: transform.FromField("Leafs")},
			{Name: "in_msgs", Type: proto.ColumnType_INT, Transform: transform.FromField("InMsgs")},
			{Name: "out_msgs", Type: proto.ColumnType_INT, Transform: transform.FromField("OutMsgs")},
			{Name: "in_bytes", Type: proto.ColumnType_INT, Transform: transform.FromField("InBytes")},
			{Name: "out_bytes", Type: proto.ColumnType_INT, Transform: transform.FromField("OutBytes")},
			{Name: "slow_consumers", Type: proto.ColumnType_INT, Transform: transform.FromField("SlowConsumers")},
			{Name: "subscriptions", Type: proto.ColumnType_INT, Transform: transform.FromField("Subscriptions")},
			{Name: "http_req_stats", Type: proto.ColumnType_JSON, Transform: transform.FromField("HTTPReqStats")},
			{Name: "config_load_time", Type: proto.ColumnType_TIMESTAMP, Transform: transform.FromField("ConfigLoadTime")},
			{Name: "tags", Type: proto.ColumnType_STRING, Transform: transform.FromField("Tags")},
			{Name: "trusted_operators_jwt", Type: proto.ColumnType_STRING, Transform: transform.FromField("TrustedOperatorsJwt")},
			{Name: "system_account", Type: proto.ColumnType_STRING, Transform: transform.FromField("SystemAccount")},
			{Name: "pinned_account_fails", Type: proto.ColumnType_INT, Transform: transform.FromField("PinnedAccountFail")},
		},
	}
}

func listVarzInfos(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	config, err := GetConfig(d.Connection)
	if err != nil {
		return nil, err
	}

	client := http.Client{
		Timeout: 10 * time.Second,
	}

	url := fmt.Sprintf("%s/varz", String(config.MonitoringURL))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	var varz server.Varz
	err = json.NewDecoder(resp.Body).Decode(&varz)
	if err != nil {
		return nil, err
	}

	d.StreamListItem(ctx, &varz)

	return nil, nil
}

func getVarzInfo(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	config, err := GetConfig(d.Connection)
	if err != nil {
		return nil, err
	}

	client := http.Client{
		Timeout: 10 * time.Second,
	}

	url := fmt.Sprintf("%s/varz", String(config.MonitoringURL))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	var varz server.Varz
	err = json.NewDecoder(resp.Body).Decode(&varz)
	if err != nil {
		return nil, err
	}

	return varz, nil
}
