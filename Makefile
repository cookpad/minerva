DEPLOY_CONFIG ?= deploy.jsonnet
STACK_CONFIG ?= stack.jsonnet

CODE_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
CWD := ${CURDIR}
BINPATH := $(CWD)/build/createPartition $(CWD)/build/listParquet $(CWD)/build/mergeParquet $(CWD)/build/apiHandler $(CWD)/build/errorHandler
SRC := $(CODE_DIR)/internal/*.go

TEMPLATE_FILE := template.json
SAM_FILE := sam.yml
BASE_FILE := $(CODE_DIR)/template.libsonnet
OUTPUT_FILE := $(CWD)/output.json

STACK_NAME := $(shell jsonnet $(DEPLOY_CONFIG) | jq .StackName)
BUILD_OPT :=

all: $(OUTPUT_FILE)

clean:
	rm -f $(BINPATH)

vendor:
	cd $(CODE_DIR) && go mod vendor && cd $(CWD)

build: $(BINPATH)

testplugin:
	cd $(CODE_DIR) && go test -v ./internal && cd $(CWD)

$(CWD)/build/createPartition: $(CODE_DIR)/lambda/createPartition/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/createPartition $(CODE_DIR)/lambda/createPartition && cd $(CWD)
$(CWD)/build/listParquet: $(CODE_DIR)/lambda/listParquet/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/listParquet $(CODE_DIR)/lambda/listParquet && cd $(CWD)
$(CWD)/build/mergeParquet: $(CODE_DIR)/lambda/mergeParquet/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/mergeParquet $(CODE_DIR)/lambda/mergeParquet && cd $(CWD)
$(CWD)/build/apiHandler: $(CODE_DIR)/lambda/apiHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/apiHandler $(CODE_DIR)/lambda/apiHandler && cd $(CWD)
$(CWD)/build/errorHandler: $(CODE_DIR)/lambda/errorHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/errorHandler $(CODE_DIR)/lambda/errorHandler && cd $(CWD)

$(TEMPLATE_FILE): $(STACK_CONFIG) $(BASE_FILE)
	jsonnet -J $(CODE_DIR) $(STACK_CONFIG) -o $(TEMPLATE_FILE)

$(SAM_FILE): $(TEMPLATE_FILE) $(BINPATH) $(DEPLOY_CONFIG)
	aws cloudformation package \
		--region $(shell jsonnet $(DEPLOY_CONFIG) | jq .Region) \
		--template-file $(TEMPLATE_FILE) \
		--s3-bucket $(shell jsonnet $(DEPLOY_CONFIG) | jq .CodeS3Bucket) \
		--s3-prefix $(shell jsonnet $(DEPLOY_CONFIG) | jq .CodeS3Prefix) \
		--output-template-file $(SAM_FILE)

$(OUTPUT_FILE): $(SAM_FILE)
	aws cloudformation deploy \
		--region $(shell jsonnet $(DEPLOY_CONFIG) | jq .Region) \
		--template-file $(SAM_FILE) \
		--stack-name $(STACK_NAME) \
		--capabilities CAPABILITY_IAM \
		--no-fail-on-empty-changeset
	aws cloudformation describe-stack-resources \
		--region $(shell jsonnet $(DEPLOY_CONFIG) | jq .Region) \
		--stack-name $(STACK_NAME) > $(OUTPUT_FILE)


delete:
	aws cloudformation delete-stack \
		--region $(shell jsonnet $(DEPLOY_CONFIG) | jq .Region) \
		--stack-name $(STACK_NAME)
	rm -f $(OUTPUT_FILE)