.PHONY: generate test race integration integration-module integration-cross integration-usecase lint security build migrate-up

generate:
	protoc -I api/proto --go_out=. --go_opt=module=github.com/lihongjie0209/go-scheduler --go-grpc_out=. --go-grpc_opt=module=github.com/lihongjie0209/go-scheduler api/proto/scheduler/v1/scheduler.proto

test:
	go test ./...

race:
	go test -race ./...

integration:
	$(MAKE) integration-module
	$(MAKE) integration-cross
	$(MAKE) integration-usecase

integration-module:
	go test -tags=integration -count=1 -timeout=10m ./migrations ./internal/store ./internal/discovery ./internal/rpc ./pkg/executor

integration-cross:
	go test -tags=integration -count=1 -timeout=10m -run TestCrossModule ./internal/integration

integration-usecase:
	go test -tags=integration -count=1 -timeout=10m -run 'Test.*UseCase' ./internal/integration

lint:
	golangci-lint run ./...

security:
	govulncheck ./...

build:
	go build -o bin/scheduler-server ./cmd/scheduler-server
	go build -o bin/api-server ./cmd/api-server
	go build -o bin/scheduler-core ./cmd/scheduler-core
	go build -o bin/schedulerctl ./cmd/schedulerctl
	go build -o bin/script-executor ./cmd/script-executor

migrate-up:
	go run ./cmd/migrate
