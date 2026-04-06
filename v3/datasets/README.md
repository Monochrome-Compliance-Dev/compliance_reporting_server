# Datasets

Handles raw data ingestion and dataset-level metadata.

Owns:

- CSV uploads
- Dataset records and metadata
- Dataset role assignment (payment_transaction, invoice_transaction, etc.)
- Import lifecycle (raw → mapped readiness)

Does NOT own:

- Mapping logic
- Business interpretation
- Canonical records

Notes:
This is the entry point for all data into the system.
