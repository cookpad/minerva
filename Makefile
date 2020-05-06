CODE_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
CWD := ${CURDIR}
BINPATH := $(CWD)/build/makePartition $(CWD)/build/listIndexObject $(CWD)/build/mergeIndexObject $(CWD)/build/apiHandler $(CWD)/build/errorHandler
SRC := $(CODE_DIR)/internal/*.go $(CODE_DIR)/pkg/*/*.go

all: build

clean:
	rm -f $(BINPATH)

build: $(BINPATH)

$(CWD)/build/makePartition: $(CODE_DIR)/lambda/makePartition/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/makePartition $(CODE_DIR)/lambda/makePartition && cd $(CWD)
$(CWD)/build/listIndexObject: $(CODE_DIR)/lambda/listIndexObject/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/listIndexObject $(CODE_DIR)/lambda/listIndexObject && cd $(CWD)
$(CWD)/build/mergeIndexObject: $(CODE_DIR)/lambda/mergeIndexObject/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/mergeIndexObject $(CODE_DIR)/lambda/mergeIndexObject && cd $(CWD)
$(CWD)/build/apiHandler: $(CODE_DIR)/lambda/apiHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/apiHandler $(CODE_DIR)/lambda/apiHandler && cd $(CWD)
$(CWD)/build/errorHandler: $(CODE_DIR)/lambda/errorHandler/*.go $(SRC)
	cd $(CODE_DIR) && env GOARCH=amd64 GOOS=linux go build -v $(BUILD_OPT) -o $(CWD)/build/errorHandler $(CODE_DIR)/lambda/errorHandler && cd $(CWD)
