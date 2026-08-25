#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <openapi-path> [method]" >&2
  echo "example: $0 '/contracts/public/items/{contract_id}' get" >&2
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 64
fi

for dependency in curl jq; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "missing dependency: $dependency" >&2
    exit 69
  fi
done

operation_path=$1
method=${2:-get}
method=$(printf '%s' "$method" | tr '[:upper:]' '[:lower:]')
openapi_url=${ESI_OPENAPI_URL:-https://esi.evetech.net/meta/openapi.json}
user_agent=${ESI_USER_AGENT:-hazardous-killbot-esi-skill/1.0\ \(+https://github.com/SvenBrnn/hazardous-killbot\)}

curl -fsSL --max-time 30 --user-agent "$user_agent" "$openapi_url" |
  jq --arg operation_path "$operation_path" --arg method "$method" '
    . as $document
    | $document.paths[$operation_path][$method] as $operation
    | if $operation == null then
        error("operation not found: \($method | ascii_upcase) \($operation_path)")
      else
        {
          openapi_version: $document.openapi,
          operation_path: $operation_path,
          method: ($method | ascii_upcase),
          operation_id: $operation.operationId,
          summary: $operation.summary,
          security: ($operation.security // []),
          parameters: [
            $operation.parameters[]?
            | if has("$ref") then {ref: .["$ref"]}
              else {name, in, required: (.required // false), schema}
              end
          ],
          responses: [
            $operation.responses
            | to_entries[]
            | {
                status: .key,
                description: .value.description,
                headers: ((.value.headers // {}) | keys)
              }
          ],
          cache: {
            age: $operation["x-cache-age"],
            client_ttl: $operation["x-client-cache-ttl"],
            server_mode: $operation["x-server-cache-mode"],
            server_ttl: $operation["x-server-cache-ttl"]
          },
          rate_limit: {
            declared: ($operation | has("x-rate-limit")),
            policy: $operation["x-rate-limit"]
          },
          compatibility_date: $operation["x-compatibility-date"]
        }
      end
  '
