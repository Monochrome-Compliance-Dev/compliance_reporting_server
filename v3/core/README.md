# Core

Shared infrastructure and cross-cutting concerns.

Owns:

- Base utilities used across v3 (non-domain specific)
- Common helpers that do not belong to a specific feature
- System-wide constants and configuration

Does NOT own:

- Business logic
- Domain-specific processing

Notes:
Keep this minimal. If something feels domain-specific, it belongs elsewhere.
