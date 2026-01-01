#!/bin/bash
# Fetch killmail data from zkillboard + ESI and save as fixture
#
# Usage: ./scripts/fetch-killmail.sh <kill_id> <name>
# Example: ./scripts/fetch-killmail.sh 119689329 nid_solo
#
# Output: resources/<kill_id>_<name>.json

set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <kill_id> <name>"
    echo "Example: $0 119689329 nid_solo"
    exit 1
fi

KILL_ID=$1
NAME=$2
OUTPUT_FILE="resources/${KILL_ID}_${NAME}.json"

echo "Fetching killmail $KILL_ID..."

# Step 1: Fetch from zkillboard to get the hash and zkb metadata
ZKB_RESPONSE=$(curl -s "https://zkillboard.com/api/killID/${KILL_ID}/")

# zkillboard returns an array, extract first element
ZKB_DATA=$(echo "$ZKB_RESPONSE" | jq '.[0].zkb')

if [ "$ZKB_DATA" == "null" ] || [ -z "$ZKB_DATA" ]; then
    echo "Error: Could not fetch zkb data for kill $KILL_ID"
    echo "Response: $ZKB_RESPONSE"
    exit 1
fi

HASH=$(echo "$ZKB_DATA" | jq -r '.hash')
echo "Got hash: $HASH"

# Step 2: Fetch full killmail from ESI
ESI_RESPONSE=$(curl -s "https://esi.evetech.net/latest/killmails/${KILL_ID}/${HASH}/")

# Check for ESI error
if echo "$ESI_RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
    echo "Error from ESI: $(echo "$ESI_RESPONSE" | jq -r '.error')"
    exit 1
fi

echo "Got ESI killmail data"

# Step 3: Combine into ZkData format
# The ZkData structure expects:
# {
#   "killID": ...,
#   "killmail": { ESI data },
#   "zkb": { zkb data }
# }
jq -n \
    --argjson killmail "$ESI_RESPONSE" \
    --argjson zkb "$ZKB_DATA" \
    --arg kill_id "$KILL_ID" \
    '{
        killID: ($kill_id | tonumber),
        killmail: $killmail,
        zkb: $zkb
    }' > "$OUTPUT_FILE"

echo "Saved to $OUTPUT_FILE"

# Show summary
VICTIM_SHIP=$(echo "$ESI_RESPONSE" | jq -r '.victim.ship_type_id')
ATTACKER_COUNT=$(echo "$ESI_RESPONSE" | jq '.attackers | length')
TOTAL_VALUE=$(echo "$ZKB_DATA" | jq -r '.totalValue')
SYSTEM_ID=$(echo "$ESI_RESPONSE" | jq -r '.solar_system_id')

echo ""
echo "Summary:"
echo "  Victim ship type: $VICTIM_SHIP"
echo "  Attackers: $ATTACKER_COUNT"
echo "  Value: $TOTAL_VALUE ISK"
echo "  System: $SYSTEM_ID"
