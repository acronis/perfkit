# Directory containing the Makefile.
PROJECT_ROOT = $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

export PATH := $(GOBIN):$(PATH)

BENCH_FLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem

# Directories containing independent Go modules.
MODULE_DIRS = benchmark acronis-db-bench acronis-restrelay-bench

GO_INSTALLABLE_DIRS = acronis-db-bench

# Directories that we want to test and track coverage for.
TEST_DIRS = benchmark acronis-restrelay-bench acronis-db-bench

include acronis-restrelay-bench/restrelay-bench.Makefile

.PHONY: all
all: lint cover

.PHONY: lint
lint: golangci-lint

.PHONY: golangci-lint
golangci-lint:
	@$(foreach mod,$(MODULE_DIRS), \
		(cd $(mod) && \
		echo "[lint] golangci-lint: $(mod)" && \
		golangci-lint run --path-prefix $(mod)) &&) true

.PHONY: tidy
tidy:
	@$(foreach dir,$(MODULE_DIRS), \
		(cd $(dir) && go mod tidy) &&) true

.PHONY: test
test:
	@$(foreach dir,$(TEST_DIRS),(cd $(dir) && go test -race ./...) &&) true

.PHONY: cover
cover:
	@mkdir -p coverage
	@echo "Running coverage tests..."
	@$(foreach dir,$(TEST_DIRS), ( \
		cd $(dir) && \
		go test -race -coverprofile=../coverage/$(dir).out -coverpkg=./... ./... && \
		go tool cover -html=../coverage/$(dir).out -o ../coverage/$(dir).html) &&) true
	@echo "Generating total coverage..."
	@echo "mode: atomic" > coverage/total.out
	@$(foreach dir,$(TEST_DIRS), \
		tail -n +2 coverage/$(dir).out >> coverage/total.out &&) true
	@go tool cover -html=coverage/total.out -o coverage/total.html
	@echo "Generating coverage summary table..."
	@echo "+-------------------------+----------+---------------------------+"
	@echo "| Directory               | Coverage | HTML Report               |"
	@echo "+-------------------------+----------+---------------------------+"
	@$(foreach dir,$(TEST_DIRS), \
		printf "| %-23s | %-8s | %-25s |\n" "$(dir)" "$$(go tool cover -func=coverage/$(dir).out | grep 'total:' | awk '{print $$3}')" "coverage/$(dir).html" &&) true
	@echo "+-------------------------+----------+---------------------------+"
	@printf "| %-23s | %-8s | %-25s |\n" "Total" "$$(go tool cover -func=coverage/total.out | grep 'total:' | awk '{print $$3}')" "coverage/total.html"
	@echo "+-------------------------+----------+---------------------------+"

.PHONY: install
install: go-install

.PHONY: go-install
go-install:
	@$(foreach dir,$(GO_INSTALLABLE_DIRS), ( \
	    cd $(dir) && \
		go install -v -ldflags "-X main.Version=`cat VERSION`" ./... \
		&& echo `go list -f '{{.Module.Path}}'`-v`cat VERSION` has been installed to `go list -f '{{.Target}}'`) &&) true

.PHONY: install-acronis-db-bench
install-acronis-db-bench:
	@$(MAKE) GO_INSTALLABLE_DIRS=acronis-db-bench install

.PHONY: build-acronis-restrelay-bench
build-acronis-restrelay-bench: build-restrelay-bench

.PHONY: up-version
up-version:
	@$(foreach dir,$(MODULE_DIRS), ( \
	    cd $(dir) ; \
		echo "Up version for `basename $(dir)` to `cat VERSION`" ; \
		git tag "`basename $(dir)`/v`cat VERSION`" ; \
		git push origin "`basename $(dir)`/v`cat VERSION`") ; \
	) true