# Obligations

Represents invoice-led (what is owed) data.

Owns:

- Canonical obligation (invoice) records
- Invoice identity and references
- Due dates and obligation attributes
- Open vs settled tracking inputs

Does NOT own:

- Payment records
- Matching logic
- PTRS calculations

Notes:
This is the foundation for ageing and outstanding analysis.
