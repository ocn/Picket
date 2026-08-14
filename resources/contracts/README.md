# Public ESI contract fixtures

`public-contract-summary.json` and `public-contract-items.json` are a single response item captured from EVE's unauthenticated public-contract endpoints for The Forge (`10000002`) on 2026-08-14. The capture deliberately retains only the fields needed to prove deserialization and uses no credentials.

At capture, the summary response returned `X-Pages: 35`, `Cache-Control: public`, `ETag`, `Expires`, and `Last-Modified`; the item response returned the same cache representation fields. The offline wire tests cover those representation headers, plus `204`, `304`, `404`, `429`, `420`, and `5xx` behavior without network access.

The fixture tests are `http_esi_accepts_a_public_contract_when_false_for_corporation_is_omitted` and `http_esi_preserves_public_blueprint_item_facts` in `tests/test_contract_intelligence.rs`.
