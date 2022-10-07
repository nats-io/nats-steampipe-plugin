#!/usr/bin/env bash

rm -rf ~/.steampipe/plugins/local/steampipe-plugin-nats/steampipe-plugin-nats.plugin
rm -rf steampipe-plugin-nats.plugin
make mac
cp steampipe-plugin-nats.plugin ~/.steampipe/plugins/local/steampipe-plugin-nats/
cp steampipe-plugin-nats.spc ~/.steampipe/config/steampipe-plugin-nats.spc