.PHONY: bench-append bench-append-1 bench-append-10 bench-append-100 bench-append-all
.PHONY: bench-append-cond bench-append-cond-1 bench-append-cond-10 bench-append-cond-100 bench-append-cond-all
.PHONY: bench-append-with-readers
# Default EVENTS_PER_REQUEST to 10 if not provided
EVENTS_PER_REQUEST ?= 10

bench-append:
	@echo "Running benchmark with EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST)"
	@trap 'kill 0' INT TERM; \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_bench && \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) python ./crates/benches/benches/grpc_append_bench_plot.py

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

bench-append-cond:
	@echo "Running conditional append benchmark with EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST)"
	@trap 'kill 0' INT TERM; \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_cond_bench && \
	EVENTS_PER_REQUEST=$(EVENTS_PER_REQUEST) MAX_THREADS=$(MAX_THREADS) python ./crates/benches/benches/grpc_append_cond_bench_plot.py

bench-append-cond-1:
	$(MAKE) bench-append-cond EVENTS_PER_REQUEST=1

bench-append-cond-10:
	$(MAKE) bench-append-cond EVENTS_PER_REQUEST=10

bench-append-cond-100:
	$(MAKE) bench-append-cond EVENTS_PER_REQUEST=100

bench-append-cond-all:
	$(MAKE) bench-append-cond-1
	$(MAKE) bench-append-cond-10
	$(MAKE) bench-append-cond-100

bench-append-with-readers:
	@echo "Running append with readers benchmark"
	@trap 'kill 0' INT TERM; \
	MAX_THREADS=$(MAX_THREADS) cargo bench -p umadb-benches --bench grpc_append_with_readers_bench && \
	python ./crates/benches/benches/grpc_append_with_readers_bench_plot.py
