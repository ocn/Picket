# Domain Docs

How engineering skills consume this repository's domain documentation.

## Before exploring, read these

- `CONTEXT.md` at the repository root
- Relevant decisions under `docs/adr/`
- `CONTEXT-MAP.md`, if one is added later, followed by each context it references

If an optional file does not exist, proceed silently. Domain-modeling creates missing documentation only when the domain work requires it.

## File structure

This is a single-context repository:

```text
/
├── CONTEXT.md
├── docs/adr/
└── src/
```

## Use the glossary's vocabulary

Use terms defined in `CONTEXT.md` in issue titles, specifications, tests, and implementation. Do not drift to synonyms that the glossary explicitly avoids.

If a required concept is absent, reconsider whether it is established project language or record the gap for domain-modeling.

## Flag ADR conflicts

Surface any conflict with an existing ADR explicitly rather than silently overriding the decision.
