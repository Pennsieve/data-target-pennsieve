.PHONY: help build docker-build docker-push clean test

SERVICE_NAME  := data-target-pennsieve
IMAGE_NAME    := pennsieve/$(SERVICE_NAME)
IMAGE_TAG     ?= latest
WORKING_DIR   ?= $(shell pwd)

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make build          - build Go binary locally"
	@echo "make test           - run tests"
	@echo "make docker-build   - build Docker image"
	@echo "make docker-push    - build and push Docker image"
	@echo "make clean          - remove build artifacts"

build:
	@echo "Building $(SERVICE_NAME)..."
	go build -o $(WORKING_DIR)/$(SERVICE_NAME) $(WORKING_DIR)
	@echo "Done: $(SERVICE_NAME)"

test:
	go test -v ./...

docker-build:
	@echo "Building Docker image $(IMAGE_NAME):$(IMAGE_TAG)..."
	DOCKER_BUILDKIT=1 docker build \
		--platform=linux/amd64 \
		-t $(IMAGE_NAME):$(IMAGE_TAG) \
		-t $(IMAGE_NAME):latest \
		$(WORKING_DIR)
	@echo "Done: $(IMAGE_NAME):$(IMAGE_TAG)"

docker-push: docker-build
	docker push $(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(IMAGE_NAME):latest

clean:
	rm -f $(WORKING_DIR)/$(SERVICE_NAME)
