# Restrict message narrative analysis to read-only tools

LLM analysis of monitored-channel messages must have only read-only tools because message content can carry prompt injections. Enforce this restriction at the tool boundary rather than relying on model instructions; application code owns persistence of message histories and analysis results. This separates narrative analysis from write authority while still allowing histories to be serialized to disk.

Read access is limited to the history being summarized, with ownership checks enforced by application code. The LLM receives no arbitrary filesystem, SQL, shell, or URL-fetching tools. The identity of the history owner remains to be defined.

Read-only access does not by itself prevent disclosure or misleading summaries. Sanitization is specified separately in ADR 0006.

Reference: [OWASP Prompt Injection Prevention](https://cheatsheetseries.owasp.org/cheatsheets/LLM_Prompt_Injection_Prevention_Cheat_Sheet.html).
