# NATS Steampipe Plugin

A plugin for [Steampipe](https://steampipe.io) for querying information about various NATS assets.

## Development

### Local testing

This requires [Steampipe](https://steampipe.io/downloads) to be installed.

Run `make link` which will build the plugin, create a `steampipe-plugin-nats.spc.dev` file, and create symlinks under the correct `~/.steampipe` directory. This `spc.dev` file is ignored from version control so you can edit freely for testing. This includes connection configuration such as the NATS URLs, the credential file, etc.
