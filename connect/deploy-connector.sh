#!/bin/bash

# Deploy Debezium connector to Kafka Connect cluster
# Usage: ./connect/deploy-connector.sh [host:port]
#
# Example:
#   ./connect/deploy-connector.sh localhost:8083
#

set -e

CONNECT_HOST="${1:-localhost:8083}"
CONNECTOR_NAME="stock-news-connector"
CONFIG_FILE="connect/debezium-connector.json"

echo "========================================"
echo "Deploying Debezium CDC Connector"
echo "========================================"
echo "Connect Host: $CONNECT_HOST"
echo "Connector Config: $CONFIG_FILE"
echo ""

# Check if connector already exists
echo "Checking if connector exists..."
if curl -s -f "http://$CONNECT_HOST/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "✅ Connector '$CONNECTOR_NAME' already exists."
    echo ""
    echo "Options:"
    echo "  1. Delete and redeploy (./connect/deploy-connector.sh delete)"
    echo "  2. Update config (./connect/deploy-connector.sh update)"
    echo ""
    read -p "What would you like to do? (skip/delete/update) [skip]: " action
    action="${action:-skip}"
    
    if [ "$action" = "delete" ]; then
        echo "Deleting connector..."
        curl -X DELETE "http://$CONNECT_HOST/connectors/$CONNECTOR_NAME"
        echo "✅ Connector deleted"
        sleep 2
    elif [ "$action" = "update" ]; then
        echo "Updating connector config..."
        curl -X PUT "http://$CONNECT_HOST/connectors/$CONNECTOR_NAME/config" \
            -H "Content-Type: application/json" \
            -d @"$CONFIG_FILE"
        echo "✅ Connector config updated"
        exit 0
    else
        echo "Skipping deployment"
        exit 0
    fi
fi

# Deploy connector
echo "Deploying connector..."
curl -X POST "http://$CONNECT_HOST/connectors" \
    -H "Content-Type: application/json" \
    -d @"$CONFIG_FILE"

echo ""
echo "✅ Connector deployed successfully!"
echo ""
echo "Waiting for connector to initialize..."
sleep 5

# Get connector status
echo ""
echo "Connector Status:"
curl -s "http://$CONNECT_HOST/connectors/$CONNECTOR_NAME/status" | jq .

echo ""
echo "========================================"
echo "Next Steps:"
echo "========================================"
echo "1. Monitor connector logs:"
echo "   docker logs pipeline_debezium_1 -f"
echo ""
echo "2. View Kafka topics:"
echo "   docker exec pipeline_redpanda rpk topic list"
echo ""
echo "3. Consume CDC events:"
echo "   docker exec pipeline_redpanda rpk topic consume stock_news_events --format=json"
echo ""
