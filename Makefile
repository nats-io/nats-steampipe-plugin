PROJECT_NAME := "nats-steampipe-plugin"

dep:
	go mod tidy

build: linux windows mac

linux: dep
	CGO_ENABLED=0 GOOS=linux go build -ldflags '-w -extldflags "-static"' -o $(PROJECT_NAME).plugin

windows: dep
	CGO_ENABLED=0 GOOS=windows go build -ldflags '-w -extldflags "-static"' -o $(PROJECT_NAME).plugin

mac: dep
	CGO_ENABLED=0 GOOS=darwin go build -ldflags '-w -extldflags "-static"' -o $(PROJECT_NAME).plugin