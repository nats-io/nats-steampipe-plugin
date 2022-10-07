connection "nats" {
  plugin = "local/steampipe-plugin-nats"
  urls = "nats://localhost:4222"
  monitoring_urls = "http://localhost:8222"
}