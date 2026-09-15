BEGIN;

CREATE TABLE IF NOT EXISTS "tbl_ptrs_reporting_entity_snapshot" (
  "id" varchar(10) PRIMARY KEY,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "profileId" varchar(10),
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
  CONSTRAINT "ptrs_reporting_entity_snapshot_ptrs_fk"
    FOREIGN KEY ("ptrsId") REFERENCES "tbl_ptrs"("id") ON DELETE CASCADE,
  CONSTRAINT "ptrs_reporting_entity_snapshot_abn_ck"
    CHECK (regexp_replace("abn", '\D', '', 'g') ~ '^\d{11}$')
);

CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_reporting_entity_snapshot_ptrs_ux"
  ON "tbl_ptrs_reporting_entity_snapshot" ("ptrsId");
CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_reporting_entity_snapshot_scope_ux"
  ON "tbl_ptrs_reporting_entity_snapshot" ("customerId", "ptrsId");
CREATE INDEX IF NOT EXISTS "ptrs_reporting_entity_snapshot_customer_idx"
  ON "tbl_ptrs_reporting_entity_snapshot" ("customerId");
CREATE INDEX IF NOT EXISTS "ptrs_reporting_entity_snapshot_profile_idx"
  ON "tbl_ptrs_reporting_entity_snapshot" ("profileId");
CREATE INDEX IF NOT EXISTS "ptrs_reporting_entity_snapshot_abn_idx"
  ON "tbl_ptrs_reporting_entity_snapshot" ("abn");
CREATE INDEX IF NOT EXISTS "ptrs_reporting_entity_snapshot_name_idx"
  ON "tbl_ptrs_reporting_entity_snapshot" ("entityName");

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ptrs_reporting_entity_snapshot_ptrs_fk'
      AND conrelid = 'tbl_ptrs_reporting_entity_snapshot'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_reporting_entity_snapshot"
      ADD CONSTRAINT "ptrs_reporting_entity_snapshot_ptrs_fk"
      FOREIGN KEY ("ptrsId") REFERENCES "tbl_ptrs"("id") ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'ptrs_reporting_entity_snapshot_abn_ck'
      AND conrelid = 'tbl_ptrs_reporting_entity_snapshot'::regclass
  ) THEN
    ALTER TABLE "tbl_ptrs_reporting_entity_snapshot"
      ADD CONSTRAINT "ptrs_reporting_entity_snapshot_abn_ck"
      CHECK (regexp_replace("abn", '\D', '', 'g') ~ '^\d{11}$');
  END IF;
END $$;

ALTER TABLE "tbl_ptrs_reporting_entity_snapshot" ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ptrs_reporting_entity_snapshot_rls_policy"
  ON "tbl_ptrs_reporting_entity_snapshot";
CREATE POLICY "ptrs_reporting_entity_snapshot_rls_policy"
  ON "tbl_ptrs_reporting_entity_snapshot" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE "tbl_ptrs_reporting_entity_snapshot" FORCE ROW LEVEL SECURITY;

COMMIT;
