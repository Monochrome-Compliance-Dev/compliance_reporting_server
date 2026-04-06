# PTRS

Payment Times Reporting Scheme logic and outputs.

Owns:

- PTRS-specific calculations
- Payment timing metrics (e.g. P95)
- Regulator-aligned rules
- Report generation inputs

Does NOT own:

- Data ingestion
- Mapping
- Matching logic

Notes:
This is a consumer of processed data, not a source.
