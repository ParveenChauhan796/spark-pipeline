# Java 17 is pinned for this project only (the shell default may be another JDK).
# Override with: make test JAVA17_HOME=/path/to/jdk-17
JAVA17_HOME ?= $(firstword $(wildcard /usr/lib/jvm/java-17-openjdk-*))
export JAVA_HOME := $(JAVA17_HOME)
export PATH := $(JAVA_HOME)/bin:$(PATH)

PYTHON ?= python3.11
VENV := .venv

define check_java
	@java -version 2>&1 | grep -q '"17\.' || { echo "Java 17 not found (JAVA_HOME='$(JAVA_HOME)'). Set JAVA17_HOME."; exit 1; }
endef

.PHONY: setup data run-all test bench

setup:
	$(call check_java)
	# Resolve symlinks first: a venv built from a symlinked interpreter (e.g. uv's ~/.local/bin/python3.11)
	# cannot find its stdlib.
	"$$(readlink -f "$$(command -v $(PYTHON))")" -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"

data:
	$(VENV)/bin/python -m catalog_pipeline.generate

run-all:
	$(call check_java)
	$(VENV)/bin/python -m catalog_pipeline.pipeline run-all

test:
	$(call check_java)
	$(VENV)/bin/pytest

# Optimization experiments (M7): fresh lakes under data/bench, writes docs/RESULTS.md (~20-30 min).
bench:
	$(call check_java)
	$(VENV)/bin/python -m catalog_pipeline.bench
