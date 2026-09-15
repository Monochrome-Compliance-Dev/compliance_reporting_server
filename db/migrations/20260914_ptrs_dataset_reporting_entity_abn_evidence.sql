BEGIN;

ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot"
  DROP CONSTRAINT IF EXISTS "ptrs_dataset_reporting_entity_abn_ck";

ALTER TABLE "tbl_ptrs_dataset_reporting_entity_snapshot"
  ALTER COLUMN "abn" TYPE text;

COMMIT;
