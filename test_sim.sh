#!/bin/bash

# Run simulation tests for SMT to verify all tasks are started
# and that server returns work properly in harsh environments
echo "Running standard simulation tests..."
go test -v -run=TestSimulationFastTasks ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running harsh environment test to verify server returns..."
go test -v -run=TestTaskManagerSimple_HarshEnvironment ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running server verification test..."
go test -v -run=TestServerVerification ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running race detection tests..."
go test -race -run=TestSimulationFastTasks -short ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running harsh environment test with race detection..."
go test -race -run=TestTaskManagerSimple_HarshEnvironment ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"

echo ""
echo "Running server verification with race detection..."
go test -race -run=TestServerVerification ./... 2>&1 | grep -v -E "(\[.*\] (DBG|ERR|INF|WRN)|Task (FAILED|COMPLETED)|[0-9]+(:[0-9]+)?(AM|PM) (DBG|ERR|INF|WRN))" | grep -E "(=== RUN |--- (PASS|FAIL)|FAIL:|ok |\s+.*_test.go:[0-9]+:)"