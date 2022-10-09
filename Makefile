PROJECT_NAME := "steampipe-plugin-nats"
PLUGIN_DIR := "$(HOME)/.steampipe/plugins/local/$(PROJECT_NAME)"
SPC_DIR := "$(HOME)/.steampipe/config"

GOOS=$(shell go env GOOS)
GOARCH=$(shell go env GOARCH)

dep:
	go mod tidy

build: dep
	CGO_ENABLED=0 go build -ldflags '-w -extldflags "-static"' -o dist/$(GOOS)-$(GOARCH)/$(PROJECT_NAME).plugin

dist:
	GOOS=linux make build
	GOOS=darwin make build
	GOOS=windows make build

link: build
	mkdir -p $(PLUGIN_DIR) $(SPC_DIR)
	ln -sf $(PWD)/dist/$(GOOS)-$(GOARCH)/$(PROJECT_NAME).plugin $(PLUGIN_DIR)
	cp -n $(PROJECT_NAME).spc $(PROJECT_NAME).spc.dev
	ln -sf $(PWD)/$(PROJECT_NAME).spc.dev $(SPC_DIR)/$(PROJECT_NAME).spc
