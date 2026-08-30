BEGIN;

ALTER TABLE "tbl_ptrs"
  ADD COLUMN IF NOT EXISTS "metricsMaterialRevision" bigint NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION "bump_ptrs_metrics_material_revision_from_new"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE "tbl_ptrs" report
  SET "metricsMaterialRevision" = report."metricsMaterialRevision" + 1
  FROM (
    SELECT DISTINCT "customerId", "ptrsId"
    FROM new_stage_rows
  ) affected
  WHERE report."customerId" = affected."customerId"
    AND report."id" = affected."ptrsId";
  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION "bump_ptrs_metrics_material_revision_from_old"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE "tbl_ptrs" report
  SET "metricsMaterialRevision" = report."metricsMaterialRevision" + 1
  FROM (
    SELECT DISTINCT "customerId", "ptrsId"
    FROM old_stage_rows
  ) affected
  WHERE report."customerId" = affected."customerId"
    AND report."id" = affected."ptrsId";
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS "ptrs_stage_row_metrics_revision_insert_trg"
  ON "tbl_ptrs_stage_row";
CREATE TRIGGER "ptrs_stage_row_metrics_revision_insert_trg"
AFTER INSERT ON "tbl_ptrs_stage_row"
REFERENCING NEW TABLE AS new_stage_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "bump_ptrs_metrics_material_revision_from_new"();

DROP TRIGGER IF EXISTS "ptrs_stage_row_metrics_revision_update_trg"
  ON "tbl_ptrs_stage_row";
CREATE TRIGGER "ptrs_stage_row_metrics_revision_update_trg"
AFTER UPDATE ON "tbl_ptrs_stage_row"
REFERENCING NEW TABLE AS new_stage_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "bump_ptrs_metrics_material_revision_from_new"();

DROP TRIGGER IF EXISTS "ptrs_stage_row_metrics_revision_delete_trg"
  ON "tbl_ptrs_stage_row";
CREATE TRIGGER "ptrs_stage_row_metrics_revision_delete_trg"
AFTER DELETE ON "tbl_ptrs_stage_row"
REFERENCING OLD TABLE AS old_stage_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "bump_ptrs_metrics_material_revision_from_old"();

-- Plain sequelize.sync() intentionally creates missing model tables at startup.
-- IF NOT EXISTS preserves such a table; the ALTER statements below add the
-- database-owned constraints, defaults, and tenant policy deterministically.
CREATE TABLE IF NOT EXISTS "tbl_ptrs_metrics_result" (
  "id" varchar(10) PRIMARY KEY,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "inputSignature" varchar(64) NOT NULL,
  "calculationVersion" varchar(64) NOT NULL,
  "status" varchar(20) NOT NULL,
  "aggregateResult" jsonb,
  "provenance" jsonb NOT NULL,
  "errorMessage" text,
  "createdBy" varchar(10),
  "startedAt" timestamptz NOT NULL DEFAULT now(),
  "completedAt" timestamptz,
  "createdAt" timestamptz NOT NULL DEFAULT now(),
  "updatedAt" timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE "tbl_ptrs_metrics_result"
  ALTER COLUMN "startedAt" SET DEFAULT now(),
  ALTER COLUMN "createdAt" SET DEFAULT now(),
  ALTER COLUMN "updatedAt" SET DEFAULT now();
  
  ALTER TABLE "tbl_ptrs_metrics_result"
  ALTER COLUMN "calculationVersion" TYPE varchar(64);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = '"tbl_ptrs_metrics_result"'::regclass
      AND conname = 'ptrs_metrics_result_ptrs_fk'
  ) THEN
    ALTER TABLE "tbl_ptrs_metrics_result"
      ADD CONSTRAINT "ptrs_metrics_result_ptrs_fk"
      FOREIGN KEY ("ptrsId")
      REFERENCES "tbl_ptrs"("id")
      ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = '"tbl_ptrs_metrics_result"'::regclass
      AND conname = 'ptrs_metrics_result_status_ck'
  ) THEN
    ALTER TABLE "tbl_ptrs_metrics_result"
      ADD CONSTRAINT "ptrs_metrics_result_status_ck"
      CHECK ("status" IN ('calculating', 'succeeded', 'failed'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = '"tbl_ptrs_metrics_result"'::regclass
      AND conname = 'ptrs_metrics_result_succeeded_payload_ck'
  ) THEN
    ALTER TABLE "tbl_ptrs_metrics_result"
      ADD CONSTRAINT "ptrs_metrics_result_succeeded_payload_ck"
      CHECK ("status" <> 'succeeded' OR "aggregateResult" IS NOT NULL);
  END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_metrics_result_signature_ux"
  ON "tbl_ptrs_metrics_result" (
    "customerId",
    "ptrsId",
    "inputSignature"
  );

CREATE INDEX IF NOT EXISTS "ptrs_metrics_result_scope_created_idx"
  ON "tbl_ptrs_metrics_result" (
    "customerId",
    "ptrsId",
    "createdAt"
  );

ALTER TABLE "tbl_ptrs_metrics_result" ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ptrs_metrics_result_rls_policy"
  ON "tbl_ptrs_metrics_result";
CREATE POLICY "ptrs_metrics_result_rls_policy"
  ON "tbl_ptrs_metrics_result" FOR ALL
  USING (
    "customerId" = current_setting('app.current_customer_id', true)::text
  )
  WITH CHECK (
    "customerId" = current_setting('app.current_customer_id', true)::text
  );
ALTER TABLE "tbl_ptrs_metrics_result" FORCE ROW LEVEL SECURITY;

COMMIT;
