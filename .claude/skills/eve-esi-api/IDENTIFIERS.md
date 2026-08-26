# Typed EVE identifiers

Carry the domain type in names, models, persistence keys, and boundary validation.

Keep these identities distinct:

- Contract ID, contract-item Record ID, and inventory Type ID;
- inventory Type ID and Group ID;
- Region ID, Solar System ID, NPC station/location ID, and player Structure ID;
- Character ID, Corporation ID, Alliance ID, and structure Owner ID;
- Killmail ID/hash and R2Z2 sequence number;
- content-observation time, representation-validation time, regional-completion time, domain-evidence time, and identity-resolution time.

Validate positivity/range and optionality at the wire boundary. Convert between domain types only through an explicit lookup or documented feed mapping, retaining the source and observation time.

## Verification

Use distinct fixture values for adjacent ID types, assert serialized field names and database keys, cover missing/invalid optional identities, and prove that observation/validation clocks advance only from their matching evidence.
