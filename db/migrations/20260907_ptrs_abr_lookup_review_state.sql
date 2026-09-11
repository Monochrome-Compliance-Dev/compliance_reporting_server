BEGIN;

ALTER TABLE "tbl_ptrs_abr_lookup_cache"
  ADD COLUMN IF NOT EXISTS "lookupStatus" varchar(32),
  ADD COLUMN IF NOT EXISTS "lookupError" text;

ALTER TABLE "tbl_ptrs_abr_lookup_cache"
  DROP CONSTRAINT IF EXISTS "ptrs_abr_lookup_cache_classification_ck";

ALTER TABLE "tbl_ptrs_abr_lookup_cache"
  ADD CONSTRAINT "ptrs_abr_lookup_cache_classification_ck"
  CHECK (
    "classification" IN (
      'NON_GOVERNMENT',
      'INACTIVE_GOVERNMENT',
      'ABN_NOT_CONFIRMED'
    )
  );

COMMIT;
