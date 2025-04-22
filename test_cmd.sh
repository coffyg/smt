#!/bin/bash

# Run command execution tests for SMT with and without race detection
# These tests verify that commands can execute properly while tasks are running
# in the background, even in harsh environments

echo "Running basic command execution test..."
go test -v -run=TestTaskManagerSimple_ExecuteCommand ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running command execution in harsh environment test..."
go test -v -run=TestTaskManagerSimple_ExecuteCommandHarshEnvironment ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running concurrent commands and tasks test..."
go test -v -run=TestTaskManagerSimple_ConcurrentCommandsAndTasks ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running command execution test with race detection..."
go test -race -run=TestTaskManagerSimple_ExecuteCommand ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running harsh environment command test with race detection..."
go test -race -run=TestTaskManagerSimple_ExecuteCommandHarshEnvironment ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running concurrent commands test with race detection..."
go test -race -run=TestTaskManagerSimple_ConcurrentCommandsAndTasks ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"