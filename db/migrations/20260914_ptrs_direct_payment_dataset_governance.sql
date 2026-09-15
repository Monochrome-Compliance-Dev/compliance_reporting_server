BEGIN;

ALTER TABLE "tbl_ptrs_dataset"
  ADD COLUMN IF NOT EXISTS "dateFormat" varchar(10);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ptrs_dataset_date_format_ck'
      AND conrelid = 'tbl_ptrs_dataset'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset"
      ADD CONSTRAINT "ptrs_dataset_date_format_ck"
      CHECK ("dateFormat" IS NULL OR "dateFormat" IN ('ISO', 'MDY', 'DMY'));
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS "tbl_ptrs_dataset_reporting_entity_snapshot" (
  "id" varchar(10) PRIMARY KEY,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "datasetId" varchar(10) NOT NULL,
  "entityName" varchar(255) NOT NULL,
  "abn" varchar(14) NOT NULL,
  "acn" varchar(14),
  "arbn" varchar(14),
  "country" varchar(2) DEFAULT 'AU',
  "source" varchar(64),
  "meta" jsonb,
  "createdBy" varchar(10),
  "updatedBy" varchar(10),
  "createdAt" timestamptz NOT NULL DEFAULT now(),
  "updatedAt" timestamptz NOT NULL DEFAULT now(),
  "deletedAt" timestamptz,
  CONSTRAINT "ptrs_dataset_reporting_entity_dataset_fk"
    FOREIGN KEY ("datasetId") REFERENCES "tbl_ptrs_dataset"("id") ON DELETE CASCADE,
  CONSTRAINT "ptrs_dataset_reporting_entity_ptrs_fk"
    FOREIGN KEY ("ptrsId") REFERENCES "tbl_ptrs"("id") ON DELETE CASCADE,
  CONSTRAINT "ptrs_dataset_reporting_entity_abn_ck"
    CHECK (regexp_replace("abn", '\D', '', 'g') ~ '^\d{11}$')
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ptrs_dataset_reporting_entity_dataset_fk'
      AND conrelid = 'tbl_ptrs_dataset_reporting_entity_snapshot'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot"
      ADD CONSTRAINT "ptrs_dataset_reporting_entity_dataset_fk"
      FOREIGN KEY ("datasetId") REFERENCES "tbl_ptrs_dataset"("id") ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ptrs_dataset_reporting_entity_ptrs_fk'
      AND conrelid = 'tbl_ptrs_dataset_reporting_entity_snapshot'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot"
      ADD CONSTRAINT "ptrs_dataset_reporting_entity_ptrs_fk"
      FOREIGN KEY ("ptrsId") REFERENCES "tbl_ptrs"("id") ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ptrs_dataset_reporting_entity_abn_ck'
      AND conrelid = 'tbl_ptrs_dataset_reporting_entity_snapshot'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot"
      ADD CONSTRAINT "ptrs_dataset_reporting_entity_abn_ck"
      CHECK (regexp_replace("abn", '\D', '', 'g') ~ '^\d{11}$');
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS "ptrs_dataset_reporting_entity_customer_idx"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot" ("customerId");
CREATE INDEX IF NOT EXISTS "ptrs_dataset_reporting_entity_ptrs_idx"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot" ("ptrsId");
CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_dataset_reporting_entity_dataset_ux"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot" ("datasetId");
CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_dataset_reporting_entity_scope_ux"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot" ("customerId", "ptrsId", "datasetId");
CREATE INDEX IF NOT EXISTS "ptrs_dataset_reporting_entity_abn_idx"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot" ("abn");

ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot" ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ptrs_dataset_reporting_entity_rls_policy"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot";
CREATE POLICY "ptrs_dataset_reporting_entity_rls_policy"
  ON "tbl_ptrs_dataset_reporting_entity_snapshot" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot" FORCE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS "tbl_ptrs_canonical_source_row_source_raw_row_id_idx"
  ON "tbl_ptrs_canonical_source_row" ("sourceRawRowId");

COMMIT;
