# PulseRTC developer tasks.

# Load-test matrix. Override on the command line:
#   make bench URL=http://localhost:8090
#   make bench URL=http://localhost:8090 TOKEN=$ADMIN_TOKEN
URL ?= http://localhost:8090
TOKEN ?=

.PHONY: bench
bench:
	scripts/bench.sh $(URL) $(TOKEN)
