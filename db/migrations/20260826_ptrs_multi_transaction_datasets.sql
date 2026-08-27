BEGIN;

ALTER TABLE "tbl_ptrs_dataset"
  ADD COLUMN IF NOT EXISTS "purpose" varchar(20),
  ADD COLUMN IF NOT EXISTS "sourceFormat" varchar(20),
  ADD COLUMN IF NOT EXISTS "adapterType" varchar(50),
  ADD COLUMN IF NOT EXISTS "adapterVersion" varchar(30),
  ADD COLUMN IF NOT EXISTS "referenceKind" varchar(50),
  ADD COLUMN IF NOT EXISTS "sourceGroupScope" varchar(100);

UPDATE "tbl_ptrs_dataset"
SET
  "purpose" = CASE
    WHEN lower(trim(coalesce("role", ''))) IN (
      'main', 'main_csv', 'main_xero', 'transactions', 'transaction', 'anchor'
    ) OR lower(trim(coalesce("sourceType", ''))) = 'xero'
      THEN 'transaction'
    ELSE 'reference'
  END,
  "referenceKind" = CASE
    WHEN lower(trim(coalesce("role", ''))) IN (
      'main', 'main_csv', 'main_xero', 'transactions', 'transaction', 'anchor'
    ) OR lower(trim(coalesce("sourceType", ''))) = 'xero'
      THEN NULL
    WHEN regexp_replace(lower(coalesce("role", '')), '[^a-z0-9]', '', 'g') IN (
      'vendormaster'
    ) THEN 'vendormaster'
    WHEN regexp_replace(lower(coalesce("role", '')), '[^a-z0-9]', '', 'g') IN (
      'termschanges', 'termchanges', 'paymenttermchange', 'paymenttermchanges'
    ) THEN 'termschanges'
    WHEN regexp_replace(lower(coalesce("role", '')), '[^a-z0-9]', '', 'g') IN (
      'entitystructure', 'entitymaster'
    ) THEN 'entitystructure'
    WHEN regexp_replace(lower(coalesce("role", '')), '[^a-z0-9]', '', 'g') IN (
      'invoice', 'invoices', 'invoicecsv', 'invoicescsv'
    ) THEN 'invoices'
    ELSE 'other'
  END,
  "sourceFormat" = CASE
    WHEN lower(trim(coalesce("sourceType", ''))) = 'xero' THEN 'api'
    WHEN lower(trim(coalesce("sourceType", ''))) IN ('xlsx', 'excel', 'myob_excel')
      THEN 'xlsx'
    ELSE 'csv'
  END,
  "adapterType" = CASE
    WHEN lower(trim(coalesce("sourceType", ''))) = 'xero'
      THEN 'xero_accounting_event'
    WHEN lower(trim(coalesce("role", ''))) IN (
      'main', 'main_csv', 'transactions', 'transaction', 'anchor'
    ) AND "adapterType" IS NULL
      THEN 'sap_accounting_event'
    ELSE "adapterType"
  END
WHERE "purpose" IS NULL
   OR "sourceFormat" IS NULL
   OR (
     "purpose" = 'reference'
     AND "referenceKind" IS NULL
   );

UPDATE "tbl_ptrs_dataset"
SET "role" = CASE
  WHEN "purpose" = 'transaction' THEN 'transaction'
  ELSE "referenceKind"
END;

ALTER TABLE "tbl_ptrs_dataset"
  ALTER COLUMN "purpose" SET NOT NULL,
  ALTER COLUMN "sourceFormat" SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'tbl_ptrs_dataset_purpose_ck'
      AND conrelid = 'tbl_ptrs_dataset'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset"
      ADD CONSTRAINT "tbl_ptrs_dataset_purpose_ck"
      CHECK ("purpose" IN ('transaction', 'reference'));
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'tbl_ptrs_dataset_source_format_ck'
      AND conrelid = 'tbl_ptrs_dataset'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset"
      ADD CONSTRAINT "tbl_ptrs_dataset_source_format_ck"
      CHECK ("sourceFormat" IN ('csv', 'xlsx', 'api'));
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'tbl_ptrs_dataset_reference_kind_ck'
      AND conrelid = 'tbl_ptrs_dataset'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset"
      ADD CONSTRAINT "tbl_ptrs_dataset_reference_kind_ck"
      CHECK (
        ("purpose" = 'transaction' AND "referenceKind" IS NULL)
        OR
        ("purpose" = 'reference' AND "referenceKind" IN (
          'vendormaster', 'termschanges', 'entitystructure', 'invoices', 'other'
        ))
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "tbl_ptrs_dataset_ptrs_purpose_idx"
  ON "tbl_ptrs_dataset" ("customerId", "ptrsId", "purpose");
CREATE INDEX IF NOT EXISTS "tbl_ptrs_dataset_reference_kind_idx"
  ON "tbl_ptrs_dataset" ("customerId", "ptrsId", "referenceKind")
  WHERE "purpose" = 'reference';
CREATE INDEX IF NOT EXISTS "tbl_ptrs_dataset_stable_order_idx"
  ON "tbl_ptrs_dataset" ("customerId", "ptrsId", "purpose", "createdAt", "id");

DROP INDEX IF EXISTS "ux_ptrs_field_map_canon";
CREATE UNIQUE INDEX IF NOT EXISTS "ux_ptrs_field_map_canon"
  ON "tbl_ptrs_field_map" (
    "customerId", "ptrsId", "profileId", "datasetId", "canonicalField"
  );

COMMIT;
