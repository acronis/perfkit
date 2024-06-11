# Directory containing the Makefile.
RESTRELAY_BENCH_PROJECT_ROOT = $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Directories containing independent Go modules.
RESTRELAY_MODULE_DIRS = client server kube-configurer

.PHONY: build-restrelay-bench
build-restrelay-bench:
	@$(foreach dir,$(RESTRELAY_MODULE_DIRS), ( \
	    cd $(RESTRELAY_BENCH_PROJECT_ROOT)/$(dir) && \
		go build -o $(RESTRELAY_BENCH_PROJECT_ROOT)//restrelay-bench-$(dir)) &&) true
