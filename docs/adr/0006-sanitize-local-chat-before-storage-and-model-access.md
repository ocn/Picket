# Sanitize Local chat before storage and external model access

Live EVE Local chat is sanitized before the application persists message history or sends it to an external LLM. Remove secrets and personal contact details, exclude real-world identity details, and neutrally restate abusive or graphic language while preserving who said what about whom and distinguishing allegations from established facts. Retain in-game identities for narrative continuity, plus available message identifiers and timestamps for attribution.

This deliberately sacrifices verbatim replay of sensitive content in application-owned history in favor of sanitized narrative evidence. It does not authorize modification of EVE's original chat-log files. Sanitized messages and summaries remain untrusted data; sanitization does not replace the access restrictions in ADR 0005.
