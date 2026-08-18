#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WIZARD="$ROOT_DIR/scripts/structure-resolver-provision.sh"
TEST_DIR=$(mktemp -d)
SOURCE_FILE="$TEST_DIR/wizard-functions.sh"
ENV_FILE="$TEST_DIR/resolver.env"
VALUES_FILE="$TEST_DIR/values.tsv"

test_cleanup() {
  rm -f -- "$SOURCE_FILE" "$ENV_FILE" "$VALUES_FILE"
  rmdir -- "$TEST_DIR" 2>/dev/null || true
}
trap test_cleanup EXIT

grep -q '^# STAGES — author this section. One stage() per step the human takes.$' "$WIZARD"
awk '/^banner "Structure resolver authorization"$/{exit} {print}' "$WIZARD" > "$SOURCE_FILE"
source "$SOURCE_FILE"
VALUES_FILE="$TEST_DIR/values.tsv"
trap test_cleanup EXIT

printf '%s\n' \
  'STRUCTURE_RESOLVER_ENABLED=true' \
  'STRUCTURE_RESOLVER_CHARACTER_ID=90000001' \
  'STRUCTURE_RESOLVER_REFRESH_TOKEN=fixture-refresh-secret' \
  'STRUCTURE_RESOLVER_CREDENTIAL_REVISION=2' \
  'STRUCTURE_RESOLVER_REDIRECT_URI=https://resolver.example.test/callback' \
  'EVE_CLIENT_ID=fixture-client-id' \
  'EVE_CLIENT_SECRET=fixture-client-secret' > "$ENV_FILE"
printf '%s\n' \
  $'STRUCTURE_RESOLVER_ENABLED\ttrue' \
  $'STRUCTURE_RESOLVER_CHARACTER_ID\t90000001' \
  $'STRUCTURE_RESOLVER_REFRESH_TOKEN\tfixture-refresh-secret-rotated' \
  $'STRUCTURE_RESOLVER_CREDENTIAL_REVISION\t3' \
  $'STRUCTURE_RESOLVER_REDIRECT_URI\thttps://resolver.example.test/callback' \
  $'EVE_CLIENT_ID\tfixture-client-id' \
  $'EVE_CLIENT_SECRET\tfixture-client-secret' > "$VALUES_FILE"

CLIENT_MODE=existing
first_output=$(persist_values)
CLIENT_MODE=existing
second_output=$(persist_values)

for key in \
  STRUCTURE_RESOLVER_ENABLED \
  STRUCTURE_RESOLVER_CHARACTER_ID \
  STRUCTURE_RESOLVER_REFRESH_TOKEN \
  STRUCTURE_RESOLVER_CREDENTIAL_REVISION \
  STRUCTURE_RESOLVER_REDIRECT_URI \
  EVE_CLIENT_ID \
  EVE_CLIENT_SECRET; do
  [[ "$(grep -c "^${key}=" "$ENV_FILE")" -eq 1 ]]
done
[[ "$(grep -c '^STRUCTURE_RESOLVER_CLIENT_ID=' "$ENV_FILE")" -eq 0 ]]
[[ "$(grep -c '^STRUCTURE_RESOLVER_CLIENT_SECRET=' "$ENV_FILE")" -eq 0 ]]
[[ "$(grep '^STRUCTURE_RESOLVER_REFRESH_TOKEN=' "$ENV_FILE")" == 'STRUCTURE_RESOLVER_REFRESH_TOKEN=fixture-refresh-secret-rotated' ]]
[[ "$first_output$second_output" != *fixture-refresh-secret* ]]
[[ "$first_output$second_output" != *fixture-client-secret* ]]
[[ "$(stat -f '%Lp' "$ENV_FILE")" == 600 ]]
