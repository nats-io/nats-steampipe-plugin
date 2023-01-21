connection "nats" {
  plugin = "plugins/local/steampipe-plugin-nats"
  urls = "nats://localhost:4222"
  context = ""
  monitoring_url = "http://localhost:8222"
}
