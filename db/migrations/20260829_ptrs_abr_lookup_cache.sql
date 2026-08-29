BEGIN;

CREATE TABLE IF NOT EXISTS "tbl_ptrs_abr_lookup_cache" (
  "id" varchar(10) PRIMARY KEY,
  "abn" varchar(11) NOT NULL UNIQUE,
  "classification" varchar(32) NOT NULL,
  "checkedAt" timestamptz NOT NULL,
  "expiresAt" timestamptz NOT NULL,
  "createdAt" timestamptz NOT NULL DEFAULT now(),
  "updatedAt" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "ptrs_abr_lookup_cache_classification_ck"
    CHECK ("classification" IN ('NON_GOVERNMENT', 'INACTIVE_GOVERNMENT'))
);

CREATE INDEX IF NOT EXISTS "ptrs_abr_lookup_cache_expires_idx"
  ON "tbl_ptrs_abr_lookup_cache" ("expiresAt");

COMMIT;
