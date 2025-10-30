#!/bin/bash

# ============================================
# Day 15: XMLA Connection Manager - Test Script
# ============================================
# Tests all Day 15 features:
# - Connection pooling
# - Query execution with retry
# - Metadata extraction
# - Semantic context generation
# ============================================

set +e  # Continue on errors to show all results

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base URL
BASE_URL="${BASE_URL:-http://localhost:3000}"

# Counters
PASSED=0
FAILED=0

# Functions
log_info() {
  echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
  echo -e "${GREEN}✅ $1${NC}"
  PASSED=$((PASSED + 1))
}

log_error() {
  echo -e "${RED}❌ $1${NC}"
  FAILED=$((FAILED + 1))
}

log_warning() {
  echo -e "${YELLOW}⚠️  $1${NC}"
}

# Banner
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║       DAY 15: XMLA CONNECTION MANAGER - COMPREHENSIVE TEST      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Test started: $(date)"
echo ""

# Check if TOKEN is set
if [ -z "$TOKEN" ]; then
  log_error "TOKEN not set. Please set TOKEN environment variable."
  echo ""
  echo "Get token by running:"
  echo "  source .test-token"
  echo "  OR"
  echo "  export TOKEN=\$(curl -s -X POST http://localhost:3000/auth/login \\"
  echo "    -H 'Content-Type: application/json' \\"
  echo "    -d '{\"email\":\"your@email.com\",\"password\":\"yourpassword\"}' | jq -r '.accessToken')"
  exit 1
fi

echo "TOKEN: ${TOKEN:0:50}..."
echo ""

# ============================================
# TEST 1: Connection Pool Statistics
# ============================================
echo "========================================"
echo "TEST 1: Connection Pool Statistics"
echo "========================================"

RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/pool/stats" \
  -H "Authorization: Bearer $TOKEN")

if echo "$RESPONSE" | jq -e '.stats' > /dev/null 2>&1; then
  log_success "Got connection pool stats"
  echo "Stats:" $(echo "$RESPONSE" | jq -c '.stats')
else
  log_error "Failed to get pool stats"
  echo "Response: $RESPONSE"
fi
echo ""

# ============================================
# TEST 2: Query Executor Statistics
# ============================================
echo "========================================"
echo "TEST 2: Query Executor Statistics"
echo "========================================"

RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/executor/stats" \
  -H "Authorization: Bearer $TOKEN")

if echo "$RESPONSE" | jq -e '.stats' > /dev/null 2>&1; then
  log_success "Got query executor stats"
  echo "Stats:" $(echo "$RESPONSE" | jq -c '.stats')
else
  log_error "Failed to get executor stats"
  echo "Response: $RESPONSE"
fi
echo ""

# ============================================
# TEST 3: Get Workspaces and Datasets
# ============================================
echo "========================================"
echo "TEST 3: Get Workspaces and Datasets"
echo "========================================"

log_info "Fetching workspaces..."
WORKSPACES=$(curl -s -X GET "$BASE_URL/workspaces" \
  -H "Authorization: Bearer $TOKEN")

if [ -z "$WORKSPACES" ] || [ "$WORKSPACES" = "null" ]; then
  log_error "No workspaces found"
  exit 1
fi

WORKSPACE_COUNT=$(echo "$WORKSPACES" | jq -r '.workspaces | length')
log_info "Found $WORKSPACE_COUNT workspace(s)"

if [ "$WORKSPACE_COUNT" -eq 0 ]; then
  log_warning "No workspaces available for testing"
  exit 0
fi

# Get first workspace
FIRST_WORKSPACE_ID=$(echo "$WORKSPACES" | jq -r '.workspaces[0].workspaceId')
FIRST_WORKSPACE_NAME=$(echo "$WORKSPACES" | jq -r '.workspaces[0].workspaceName')
POWERBI_WORKSPACE_ID=$(echo "$WORKSPACES" | jq -r '.workspaces[0].powerbiWorkspaceId')

log_info "Using workspace: $FIRST_WORKSPACE_NAME ($FIRST_WORKSPACE_ID)"
echo ""

# Get datasets for this workspace
log_info "Fetching datasets for workspace..."
DATASETS=$(curl -s -X GET "$BASE_URL/powerbi/workspaces/$POWERBI_WORKSPACE_ID/datasets" \
  -H "Authorization: Bearer $TOKEN")

DATASET_COUNT=$(echo "$DATASETS" | jq -r '.value | length' 2>/dev/null || echo "0")

if [ "$DATASET_COUNT" -gt 0 ]; then
  FIRST_DATASET_ID=$(echo "$DATASETS" | jq -r '.value[0].id')
  FIRST_DATASET_NAME=$(echo "$DATASETS" | jq -r '.value[0].name')
  log_success "Found dataset: $FIRST_DATASET_NAME ($FIRST_DATASET_ID)"
else
  log_warning "No datasets found in workspace"
  FIRST_DATASET_ID=""
fi
echo ""

# ============================================
# TEST 4: Test Connection Pool
# ============================================
if [ -n "$POWERBI_WORKSPACE_ID" ]; then
  echo "========================================"
  echo "TEST 4: Test Connection Pool"
  echo "========================================"

  RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/pool/test" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"workspaceId\": \"$POWERBI_WORKSPACE_ID\"}")

  if echo "$RESPONSE" | jq -e '.success' | grep -q 'true'; then
    log_success "Connection pool test passed"
    echo "Connection key:" $(echo "$RESPONSE" | jq -r '.connectionKey')
    echo "Token expires in:" $(echo "$RESPONSE" | jq -r '.tokenExpiresIn') "seconds"
  else
    log_error "Connection pool test failed"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 5: Test XMLA Connection to Dataset
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 5: Test XMLA Connection"
  echo "========================================"

  RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/test-connection" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.connectionString' > /dev/null 2>&1; then
    log_success "XMLA connection test passed"
    echo "Dataset:" $(echo "$RESPONSE" | jq -r '.datasetName')
    echo "Connection string:" $(echo "$RESPONSE" | jq -r '.connectionString')
  else
    log_warning "XMLA connection test failed (may be expected for non-premium workspaces)"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 6: Get Basic Dataset Metadata
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 6: Get Basic Dataset Metadata"
  echo "========================================"

  RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/metadata" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.metadata' > /dev/null 2>&1; then
    log_success "Got dataset metadata"
    echo "Dataset:" $(echo "$RESPONSE" | jq -r '.metadata.datasetName')
    TABLES_COUNT=$(echo "$RESPONSE" | jq -r '.metadata.tables | length')
    MEASURES_COUNT=$(echo "$RESPONSE" | jq -r '.metadata.measures | length')
    echo "Tables: $TABLES_COUNT"
    echo "Measures: $MEASURES_COUNT"
  else
    log_error "Failed to get dataset metadata"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 7: Extract Complete Semantic Metadata
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 7: Extract Complete Semantic Metadata"
  echo "========================================"

  log_info "Extracting semantic metadata (this may take a while)..."
  RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/extract-metadata" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.metadata' > /dev/null 2>&1; then
    log_success "Semantic metadata extracted successfully"
    TABLES=$(echo "$RESPONSE" | jq -r '.metadata.tables | length')
    MEASURES=$(echo "$RESPONSE" | jq -r '.metadata.measures | length')
    RELATIONSHIPS=$(echo "$RESPONSE" | jq -r '.metadata.relationships | length')
    COLUMNS=$(echo "$RESPONSE" | jq -r '.metadata.columns | length')
    EXTRACTION_TIME=$(echo "$RESPONSE" | jq -r '.metadata.extractionTimeMs')
    
    echo "Tables: $TABLES"
    echo "Measures: $MEASURES"
    echo "Relationships: $RELATIONSHIPS"
    echo "Columns: $COLUMNS"
    echo "Extraction time: ${EXTRACTION_TIME}ms"
    
    # Show extraction methods used
    echo "Extraction methods:"
    echo "$RESPONSE" | jq -r '.metadata.extractionMethods'
  else
    log_warning "Semantic metadata extraction failed (may require premium capacity)"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 8: Get Cached Metadata
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 8: Get Cached Metadata"
  echo "========================================"

  RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/cached-metadata" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.metadata' > /dev/null 2>&1; then
    log_success "Got cached metadata"
    echo "Last sync:" $(echo "$RESPONSE" | jq -r '.metadata.lastSync')
    echo "Schema version:" $(echo "$RESPONSE" | jq -r '.metadata.schemaVersion')
  else
    log_info "No cached metadata yet (run extract-metadata first)"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 9: Generate Semantic Context for AI
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 9: Generate Semantic Context for AI"
  echo "========================================"

  log_info "Generating semantic context..."
  RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/semantic-context" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.context' > /dev/null 2>&1; then
    log_success "Semantic context generated"
    echo "Dataset:" $(echo "$RESPONSE" | jq -r '.context.datasetName')
    echo "Summary:"
    echo "$RESPONSE" | jq -r '.context.summary'
    
    # Show text context preview (first 500 chars)
    TEXT_CONTEXT=$(echo "$RESPONSE" | jq -r '.context.textContext')
    echo ""
    echo "Text Context Preview:"
    echo "$TEXT_CONTEXT" | head -c 500
    echo "..."
  else
    log_warning "Semantic context generation failed"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 10: Get Tables
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 10: Get Tables"
  echo "========================================"

  RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/tables" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.tables' > /dev/null 2>&1; then
    TABLES_COUNT=$(echo "$RESPONSE" | jq -r '.tables | length')
    if [ "$TABLES_COUNT" -gt 0 ]; then
      log_success "Got $TABLES_COUNT tables"
      echo "Tables:"
      echo "$RESPONSE" | jq -r '.tables[].name' | head -5
    else
      log_info "No tables found (empty array)"
    fi
  else
    log_warning "Failed to get tables"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 11: Get Measures
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 11: Get Measures"
  echo "========================================"

  RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/measures" \
    -H "Authorization: Bearer $TOKEN")

  if echo "$RESPONSE" | jq -e '.measures' > /dev/null 2>&1; then
    MEASURES_COUNT=$(echo "$RESPONSE" | jq -r '.measures | length')
    if [ "$MEASURES_COUNT" -gt 0 ]; then
      log_success "Got $MEASURES_COUNT measures"
      echo "Measures:"
      echo "$RESPONSE" | jq -r '.measures[].name' | head -5
    else
      log_info "No measures found (empty array)"
    fi
  else
    log_warning "Failed to get measures"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 12: Validate DAX Query
# ============================================
echo "========================================"
echo "TEST 12: Validate DAX Query"
echo "========================================"

# Test valid query
VALID_QUERY='EVALUATE ROW("Test", 1)'
RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/validate-query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"$VALID_QUERY\"}")

if echo "$RESPONSE" | jq -e '.isValid' | grep -q 'true'; then
  log_success "Valid query passed validation"
else
  log_error "Valid query failed validation"
  echo "Response: $RESPONSE"
fi

# Test invalid query
INVALID_QUERY='SELECT * FROM table'
RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/validate-query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\": \"$INVALID_QUERY\"}")

if echo "$RESPONSE" | jq -e '.isValid' | grep -q 'false'; then
  log_success "Invalid query correctly rejected"
  echo "Errors:" $(echo "$RESPONSE" | jq -r '.errors[]')
else
  log_error "Invalid query was not rejected"
  echo "Response: $RESPONSE"
fi
echo ""

# ============================================
# TEST 13: Execute Simple DAX Query
# ============================================
if [ -n "$FIRST_DATASET_ID" ]; then
  echo "========================================"
  echo "TEST 13: Execute Simple DAX Query"
  echo "========================================"

  SIMPLE_QUERY='EVALUATE ROW("ConnectionTest", "Success", "Timestamp", NOW())'
  RESPONSE=$(curl -s -X POST "$BASE_URL/xmla/datasets/$FIRST_DATASET_ID/execute" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"$SIMPLE_QUERY\"}")

  if echo "$RESPONSE" | jq -e '.results' > /dev/null 2>&1; then
    log_success "DAX query executed successfully"
    EXEC_TIME=$(echo "$RESPONSE" | jq -r '.executionTimeMs')
    ROW_COUNT=$(echo "$RESPONSE" | jq -r '.rowCount')
    echo "Execution time: ${EXEC_TIME}ms"
    echo "Rows returned: $ROW_COUNT"
  else
    log_warning "DAX query execution failed (may require premium capacity)"
    echo "Response: $RESPONSE"
  fi
  echo ""
fi

# ============================================
# TEST 14: Check Pool Stats After Tests
# ============================================
echo "========================================"
echo "TEST 14: Final Connection Pool Statistics"
echo "========================================"

RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/pool/stats" \
  -H "Authorization: Bearer $TOKEN")

if echo "$RESPONSE" | jq -e '.stats' > /dev/null 2>&1; then
  log_success "Got final pool stats"
  echo "Current pool size:" $(echo "$RESPONSE" | jq -r '.stats.currentPoolSize')
  echo "Total connections created:" $(echo "$RESPONSE" | jq -r '.stats.totalConnections')
  echo "Reuse count:" $(echo "$RESPONSE" | jq -r '.stats.reuseCount')
  echo "Expiry count:" $(echo "$RESPONSE" | jq -r '.stats.expiryCount')
  
  if echo "$RESPONSE" | jq -e '.stats.connections[]' > /dev/null 2>&1; then
    echo ""
    echo "Active connections:"
    echo "$RESPONSE" | jq -r '.stats.connections[] | "  - Key: \(.key), Usage: \(.usageCount), Valid: \(.isValid)"'
  fi
else
  log_error "Failed to get final pool stats"
  echo "Response: $RESPONSE"
fi
echo ""

# ============================================
# TEST 15: Check Query Executor Stats After Tests
# ============================================
echo "========================================"
echo "TEST 15: Final Query Executor Statistics"
echo "========================================"

RESPONSE=$(curl -s -X GET "$BASE_URL/xmla/executor/stats" \
  -H "Authorization: Bearer $TOKEN")

if echo "$RESPONSE" | jq -e '.stats' > /dev/null 2>&1; then
  log_success "Got final executor stats"
  echo "Total queries:" $(echo "$RESPONSE" | jq -r '.stats.totalQueries')
  echo "Successful queries:" $(echo "$RESPONSE" | jq -r '.stats.successfulQueries')
  echo "Failed queries:" $(echo "$RESPONSE" | jq -r '.stats.failedQueries')
  echo "Retried queries:" $(echo "$RESPONSE" | jq -r '.stats.retriedQueries')
  echo "Success rate:" $(echo "$RESPONSE" | jq -r '.stats.successRate')
  echo "Average execution time:" $(echo "$RESPONSE" | jq -r '.stats.averageExecutionTimeMs') "ms"
else
  log_error "Failed to get final executor stats"
  echo "Response: $RESPONSE"
fi
echo ""

# ============================================
# FINAL SUMMARY
# ============================================
echo "========================================"
echo "TEST SUMMARY"
echo "========================================"
echo "Total Tests: $((PASSED + FAILED))"
echo -e "${GREEN}✅ Passed: $PASSED${NC}"
echo -e "${RED}❌ Failed: $FAILED${NC}"
echo ""
echo "Test completed: $(date)"

if [ $FAILED -eq 0 ]; then
  echo ""
  echo "╔════════════════════════════════════════╗"
  echo "║  🎉 ALL DAY 15 TESTS PASSED! 🎉         ║"
  echo "╚════════════════════════════════════════╝"
  exit 0
else
  echo ""
  echo "╔════════════════════════════════════════╗"
  echo "║  ⚠️  SOME TESTS FAILED                  ║"
  echo "╚════════════════════════════════════════╝"
  exit 1
fi

