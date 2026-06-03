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

.PHONY: deploy
deploy:
	kubectl apply -f manifests/namespace.yaml
	kubectl apply -f manifests/serviceaccount.yaml
	kubectl apply -f manifests/rbac.yaml
	kubectl apply -f manifests/redis-statefulset-example.yaml
	kubectl apply -f manifests/redis-write-service.yaml
	kubectl apply -f manifests/deployment.yaml

.PHONY: undeploy
undeploy:
	-kubectl delete -f manifests/deployment.yaml
	-kubectl delete -f manifests/redis-write-service.yaml
	-kubectl delete -f manifests/redis-statefulset-example.yaml
	-kubectl delete -f manifests/rbac.yaml
	-kubectl delete -f manifests/serviceaccount.yaml

.PHONY: integration-test
integration-test:
	NAMESPACE=$(NAMESPACE) ./tests/integration/run-integration-test.sh
