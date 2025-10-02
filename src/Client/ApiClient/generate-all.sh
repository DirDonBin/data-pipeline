#!/bin/bash

set -e

CONFIG_FILE="${1:-api-sources.json}"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "=== API Client Generation ==="
echo ""

# Check NSwag installation
echo "Checking NSwag installation..."
if ! dotnet tool list -g | grep -q "nswag.consolecore"; then
    echo "Installing NSwag..."
    dotnet tool install -g NSwag.ConsoleCore
    echo "NSwag installed successfully!"
else
    echo "NSwag already installed"
fi

# Load configuration
CONFIG_PATH="$SCRIPT_DIR/$CONFIG_FILE"
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Configuration file not found: $CONFIG_PATH"
    exit 1
fi

echo "Loading configuration from: $CONFIG_FILE"

# Create Generated directory
GEN_DIR="$SCRIPT_DIR/Generated"
if [ ! -d "$GEN_DIR" ]; then
    mkdir -p "$GEN_DIR"
    echo "Created directory: Generated/"
fi

# Check template
TEMPLATE_PATH="$SCRIPT_DIR/nswag-template.json"
if [ ! -f "$TEMPLATE_PATH" ]; then
    echo "Error: Template file not found: $TEMPLATE_PATH"
    exit 1
fi

TEMPLATE=$(cat "$TEMPLATE_PATH")

# Parse JSON and generate clients
GATEWAY_URL=$(jq -r '.gatewayBaseUrl' "$CONFIG_PATH")
API_COUNT=$(jq '.apis | length' "$CONFIG_PATH")

echo ""
echo "Gateway Base URL: $GATEWAY_URL"
echo "APIs to generate: $API_COUNT"
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0

for i in $(seq 0 $((API_COUNT - 1))); do
    API_NAME=$(jq -r ".apis[$i].name" "$CONFIG_PATH")
    DISPLAY_NAME=$(jq -r ".apis[$i].displayName" "$CONFIG_PATH")
    OPENAPI_PATH=$(jq -r ".apis[$i].openApiPath" "$CONFIG_PATH")
    NAMESPACE=$(jq -r ".apis[$i].namespace" "$CONFIG_PATH")
    CLIENT_CLASS=$(jq -r ".apis[$i].clientClass" "$CONFIG_PATH")
    EXCEPTION_CLASS=$(jq -r ".apis[$i].exceptionClass" "$CONFIG_PATH")
    OUTPUT_FILE=$(jq -r ".apis[$i].outputFile" "$CONFIG_PATH")
    
    echo "--- Generating $DISPLAY_NAME ---"
    echo "  Name: $API_NAME"
    echo "  URL: $GATEWAY_URL$OPENAPI_PATH"
    echo "  Namespace: $NAMESPACE"
    echo "  Output: $OUTPUT_FILE"
    
    # Replace placeholders
    CONFIG_CONTENT=$(echo "$TEMPLATE" | \
        sed "s|{URL}|$GATEWAY_URL$OPENAPI_PATH|g" | \
        sed "s|{NAMESPACE}|$NAMESPACE|g" | \
        sed "s|{CLIENT_CLASS}|$CLIENT_CLASS|g" | \
        sed "s|{EXCEPTION_CLASS}|$EXCEPTION_CLASS|g" | \
        sed "s|{OUTPUT}|$OUTPUT_FILE|g")
    
    # Save temporary config
    TEMP_CONFIG="$SCRIPT_DIR/nswag-temp-$API_NAME.json"
    echo "$CONFIG_CONTENT" > "$TEMP_CONFIG"
    
    # Run NSwag
    if nswag run "$TEMP_CONFIG" > /dev/null 2>&1; then
        echo "  Status: SUCCESS"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "  Status: FAILED"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
    
    # Clean up
    rm -f "$TEMP_CONFIG"
    
    echo ""
done

# Summary
echo "=== Generation Summary ==="
echo "  Success: $SUCCESS_COUNT"
echo "  Failed: $FAIL_COUNT"
echo "  Total: $API_COUNT"
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo "All clients generated successfully!"
    exit 0
else
    echo "Some clients failed to generate"
    exit 1
fi
