.PHONY: docker push clean docs
.DEFAULT_GOAL := maf-job-orchestrator

GO_PROGRAM=maf-job-orchestrator
VERSION=$(shell git describe --tags --dirty --always  2>/dev/null || echo "unknown")
DOCKER_REGISTRY?=hub.comcast.net
DOCKER_NAME=mesa/$(GO_PROGRAM)
DOCKERFILE=build/package/$(GO_PROGRAM).Dockerfile


$(GO_PROGRAM): $(shell find . -name \*.go)
	CGO_ENABLED=0 go build -ldflags "-X main.version=$(VERSION)" -o . ./... 

test:
	go test -v ./pkg/...

# In order to succeed passing ssh, run your ssh-agent and ssh-add your keys
docker: $(shell find . -name \*.go)
	DOCKER_BUILDKIT=1 docker build \
		--ssh default \
		--build-arg VERSION=$(VERSION) \
		-f $(DOCKERFILE) \
		-t $(DOCKER_REGISTRY)/$(DOCKER_NAME):$(VERSION) -t $(DOCKER_REGISTRY)/$(DOCKER_NAME):latest \
		.

push:
	docker push $(DOCKER_REGISTRY)/$(DOCKER_NAME):$(VERSION)
push_latest:
	docker push $(DOCKER_REGISTRY)/$(DOCKER_NAME):latest

clean:
	docker rmi $(DOCKER_REGISTRY)/$(DOCKER_NAME):$(VERSION) $(DOCKER_REGISTRY)/$(DOCKER_NAME):latest

sync: docker push clean
sync_latest: docker push_latest clean