CODE_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
CWD := ${CURDIR}

BIN_DIR := $(CODE_DIR)/build
BINPATH := \
	$(BIN_DIR)/partitioner \
	$(BIN_DIR)/merger \
	$(BIN_DIR)/apiHandler \
	$(BIN_DIR)/composer \
	$(BIN_DIR)/dispatcher \
	$(BIN_DIR)/errorHandler

SRC := $(CODE_DIR)/internal/*.go $(CODE_DIR)/internal/*/*.go  $(CODE_DIR)/pkg/*/*.go

all: build

clean:
	rm -f $(BINPATH)

build: $(BINPATH)

$(BIN_DIR)/partitioner: $(CODE_DIR)/lambda/partitioner/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/partitioner $(CODE_DIR)/lambda/partitioner && cd $(CWD)
$(BIN_DIR)/merger: $(CODE_DIR)/lambda/merger/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/merger $(CODE_DIR)/lambda/merger && cd $(CWD)
$(BIN_DIR)/composer: $(CODE_DIR)/lambda/composer/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/composer $(CODE_DIR)/lambda/composer && cd $(CWD)
$(BIN_DIR)/dispatcher: $(CODE_DIR)/lambda/dispatcher/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/dispatcher $(CODE_DIR)/lambda/dispatcher && cd $(CWD)
$(BIN_DIR)/apiHandler: $(CODE_DIR)/lambda/apiHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/apiHandler $(CODE_DIR)/lambda/apiHandler && cd $(CWD)
$(BIN_DIR)/errorHandler: $(CODE_DIR)/lambda/errorHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/errorHandler $(CODE_DIR)/lambda/errorHandler && cd $(CWD)
