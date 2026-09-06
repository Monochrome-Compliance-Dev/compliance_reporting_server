BEGIN;

-- Run with the migration administrator after draining canonical writers.
-- Fail rather than silently inspecting only one RLS tenant or stealing a job.
SET LOCAL row_security = off;
LOCK TABLE "tbl_ptrs_canonical_revision" IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "tbl_ptrs_canonical_revision" WHERE "status" = 'building') THEN
    RAISE EXCEPTION 'Canonical building revisions require operator review before migration'
      USING HINT = 'Drain writers, verify ownership has ended, and explicitly mark abandoned attempts failed. See docs/ptrs-canonical-materialisation.md. No automatic cleanup is performed.';
  END IF;
END $$;

CREATE UNIQUE INDEX "ptrs_canonical_revision_active_material_ux"
  ON "tbl_ptrs_canonical_revision" ("customerId", "ptrsId", "datasetId", "materialSignature")
  WHERE "status" IN ('building', 'succeeded');

DROP INDEX "ptrs_canonical_revision_success_material_ux";

COMMIT;
