# PTRS self-contained workbook import

`POST /api/v2/ptrs/:id/workbooks` imports an Excel workbook using the profile
selected by the PTRS report. Each configured worksheet is persisted as an
ordinary `tbl_ptrs_dataset` plus `tbl_ptrs_import_raw` rows, so the existing
Link, Map, canonical revision and Stage flow remains authoritative.

Configure the profile `meta` using this shape:

```json
{
  "workbookImport": {
    "sheets": [
      {
        "sheetName": "<exact worksheet name>",
        "headerRow": 1,
        "range": "A1:Z5000",
        "purpose": "transaction",
        "adapterType": "sap_accounting_event",
        "adapterVersion": "1",
        "sourceGroupScope": "<stable ledger scope>",
        "requiredColumns": ["<source column>"]
      },
      {
        "sheetName": "<exact worksheet name>",
        "purpose": "reference",
        "referenceKind": "vendormaster",
        "requiredColumns": ["<source column>"]
      }
    ],
    "ignoredSheets": ["<explicitly irrelevant worksheet>"]
  }
}
```

`referenceKind` uses the existing PTRS values: `vendormaster`, `termschanges`,
`entitystructure`, `invoices` or `other`. Unconfigured worksheets, missing or
duplicate configured worksheets, duplicate headings and missing required
columns fail the whole import before any dataset is committed.

Provenance is retained through the stored workbook reference, dataset metadata
(worksheet, configured role and header row), raw `rowNo`, canonical source row
and revision identifiers, and the existing Stage canonical lineage.

No Orontide or EnviroPacific worksheet or column mapping is embedded in code.
Their profile configuration must be populated from an authoritative workbook
sample before operational import.
