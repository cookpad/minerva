CODE_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
CWD := ${CURDIR}

BIN_DIR := $(CODE_DIR)/build
BINPATH := $(BIN_DIR)/makePartition $(BIN_DIR)/listIndexObject $(BIN_DIR)/mergeIndexObject $(BIN_DIR)/apiHandler $(BIN_DIR)/errorHandler
SRC := $(CODE_DIR)/internal/*.go $(CODE_DIR)/pkg/*/*.go

all: build

clean:
	rm -f $(BINPATH)

build: $(BINPATH)

$(BIN_DIR)/makePartition: $(CODE_DIR)/lambda/makePartition/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/makePartition $(CODE_DIR)/lambda/makePartition && cd $(CWD)
$(BIN_DIR)/listIndexObject: $(CODE_DIR)/lambda/listIndexObject/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/listIndexObject $(CODE_DIR)/lambda/listIndexObject && cd $(CWD)
$(BIN_DIR)/mergeIndexObject: $(CODE_DIR)/lambda/mergeIndexObject/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/mergeIndexObject $(CODE_DIR)/lambda/mergeIndexObject && cd $(CWD)
$(BIN_DIR)/apiHandler: $(CODE_DIR)/lambda/apiHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/apiHandler $(CODE_DIR)/lambda/apiHandler && cd $(CWD)
$(BIN_DIR)/errorHandler: $(CODE_DIR)/lambda/errorHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(BIN_DIR)/errorHandler $(CODE_DIR)/lambda/errorHandler && cd $(CWD)
