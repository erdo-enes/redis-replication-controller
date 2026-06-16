IMAGE ?= redis-replication-controller:latest
NAMESPACE ?= redis

# Run Go through the official golang container so a system Go install is not required.
# Override with `make GO=go ...` if you have Go locally.
GO ?= docker run --rm -v "$(CURDIR)":/src -w /src golang:1.22 go

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: test
test:
	$(GO) test ./... -count=1

.PHONY: build
build:
	$(GO) build -o bin/controller ./cmd

.PHONY: check
check: tidy vet test build

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE) .

# Manifests are numbered (00-,01-,...) so a single directory apply runs them in
# dependency order: namespaces -> ServiceAccount -> RBAC -> Redis sets ->
# controller. This deploys both example sets (cache in "redis", sessions in
# "redis-sessions") plus the external LoadBalancer.
.PHONY: deploy
deploy:
	kubectl apply -f manifests/

.PHONY: undeploy
undeploy:
	-kubectl delete --ignore-not-found -f manifests/

.PHONY: integration-test
integration-test:
	NAMESPACE=$(NAMESPACE) ./tests/integration/run-integration-test.sh
