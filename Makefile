.PHONY: generate test race integration integration-module integration-cross integration-usecase integration-cli-python integration-benchmark lint security build migrate-up

generate:
	protoc -I api/proto --go_out=. --go_opt=module=github.com/lihongjie0209/go-scheduler --go-grpc_out=. --go-grpc_opt=module=github.com/lihongjie0209/go-scheduler api/proto/scheduler/v1/scheduler.proto api/proto/executor/v1/executor.proto
	sed -i -E 's#(//[[:space:]]*(- )?protoc[[:space:]]+)v[^[:space:]]+#\1v4.25.1#' gen/*/v1/*.pb.go

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

integration-cli-python:
	python3 -m unittest -v tests.cli.test_schedulerctl

integration-benchmark:
	./hack/standalone-benchmark.sh

lint:
	golangci-lint run ./...

security:
	govulncheck ./...

build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/schedulerctl ./cmd/schedulerctl
	go build -o bin/scheduler-bench ./cmd/scheduler-bench

migrate-up:
	go run ./cmd/scheduler migrate
