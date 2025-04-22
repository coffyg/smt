#!/bin/bash

# Run all tests for SMT with clean output that only shows test run/pass/fail messages
echo "Running all tests..."
go test -v ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running all tests with race detection..."
go test -race -v ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"