.PHONY: bench-append bench-append-1 bench-append-10 bench-append-100 bench-append-all
# Default EVENTS_PER_REQUEST to 10 if not provided
EVENTS_PER_REQUEST ?= 10

bench-append:
	@echo "Running benchmark with EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST)"
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) cargo bench -p umadb-benches --bench grpc_append_bench
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) python ./crates/benches/benches/grpc_append_bench_plot.py

bench-append-1:
	$(MAKE) bench-append EVENTS_PER_REQUEST=1

bench-append-10:
	$(MAKE) bench-append EVENTS_PER_REQUEST=10

bench-append-100:
	$(MAKE) bench-append EVENTS_PER_REQUEST=100

bench-append-all:
	$(MAKE) bench-append-1
	$(MAKE) bench-append-10
	$(MAKE) bench-append-100
