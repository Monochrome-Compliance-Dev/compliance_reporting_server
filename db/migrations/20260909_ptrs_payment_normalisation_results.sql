BEGIN;

ALTER TABLE "tbl_ptrs"
  ADD COLUMN IF NOT EXISTS "normalisationInputRevision" bigint NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION "bump_ptrs_normalisation_input_revision_from_new"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE "tbl_ptrs" report
  SET "normalisationInputRevision" = report."normalisationInputRevision" + 1
  FROM (
    SELECT DISTINCT "customerId", "ptrsId"
    FROM new_stage_rows
  ) affected
  WHERE report."customerId" = affected."customerId"
    AND report."id" = affected."ptrsId";
  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION "bump_ptrs_normalisation_input_revision_from_old"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE "tbl_ptrs" report
  SET "normalisationInputRevision" = report."normalisationInputRevision" + 1
  FROM (
    SELECT DISTINCT "customerId", "ptrsId"
    FROM old_stage_rows
  ) affected
  WHERE report."customerId" = affected."customerId"
    AND report."id" = affected."ptrsId";
  RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION "bump_ptrs_normalisation_input_revision_from_update"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE "tbl_ptrs" report
  SET "normalisationInputRevision" = report."normalisationInputRevision" + 1
  FROM (
    SELECT DISTINCT changed."customerId", changed."ptrsId"
    FROM (
      SELECT new_row."customerId", new_row."ptrsId"
      FROM new_stage_rows new_row
      JOIN old_stage_rows old_row ON old_row."id" = new_row."id"
      WHERE ROW(
        new_row."customerId",
        new_row."ptrsId",
        new_row."profileId",
        new_row."datasetId",
        new_row."semanticKind",
        new_row."sourceGroupScope",
        new_row."rowNo",
        new_row."sourceAccountCode",
        new_row."description",
        new_row."documentType",
        new_row."clearingDocument",
        new_row."paymentAmount",
        new_row."paymentDate",
        new_row."invoiceIssueDate",
        new_row."invoiceReceiptDate",
        new_row."adapterType",
        new_row."data"->'company_code',
        new_row."data"->'rcti',
        new_row."meta"->'canonical'->'lineage'->'canonicalSources'->'invoice_receipt_date'
      ) IS DISTINCT FROM ROW(
        old_row."customerId",
        old_row."ptrsId",
        old_row."profileId",
        old_row."datasetId",
        old_row."semanticKind",
        old_row."sourceGroupScope",
        old_row."rowNo",
        old_row."sourceAccountCode",
        old_row."description",
        old_row."documentType",
        old_row."clearingDocument",
        old_row."paymentAmount",
        old_row."paymentDate",
        old_row."invoiceIssueDate",
        old_row."invoiceReceiptDate",
        old_row."adapterType",
        old_row."data"->'company_code',
        old_row."data"->'rcti',
        old_row."meta"->'canonical'->'lineage'->'canonicalSources'->'invoice_receipt_date'
      )
      UNION
      SELECT old_row."customerId", old_row."ptrsId"
      FROM new_stage_rows new_row
      JOIN old_stage_rows old_row ON old_row."id" = new_row."id"
      WHERE new_row."customerId" IS DISTINCT FROM old_row."customerId"
         OR new_row."ptrsId" IS DISTINCT FROM old_row."ptrsId"
    ) changed
  ) affected
  WHERE report."customerId" = affected."customerId"
    AND report."id" = affected."ptrsId";
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS "ptrs_stage_row_normalisation_revision_insert_trg"
  ON "tbl_ptrs_stage_row";
CREATE TRIGGER "ptrs_stage_row_normalisation_revision_insert_trg"
AFTER INSERT ON "tbl_ptrs_stage_row"
REFERENCING NEW TABLE AS new_stage_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "bump_ptrs_normalisation_input_revision_from_new"();

DROP TRIGGER IF EXISTS "ptrs_stage_row_normalisation_revision_update_trg"
  ON "tbl_ptrs_stage_row";
CREATE TRIGGER "ptrs_stage_row_normalisation_revision_update_trg"
AFTER UPDATE ON "tbl_ptrs_stage_row"
REFERENCING OLD TABLE AS old_stage_rows NEW TABLE AS new_stage_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "bump_ptrs_normalisation_input_revision_from_update"();

DROP TRIGGER IF EXISTS "ptrs_stage_row_normalisation_revision_delete_trg"
  ON "tbl_ptrs_stage_row";
CREATE TRIGGER "ptrs_stage_row_normalisation_revision_delete_trg"
AFTER DELETE ON "tbl_ptrs_stage_row"
REFERENCING OLD TABLE AS old_stage_rows
FOR EACH STATEMENT
EXECUTE FUNCTION "bump_ptrs_normalisation_input_revision_from_old"();

CREATE TABLE IF NOT EXISTS "tbl_ptrs_payment_normalisation_result" (
  "id" varchar(10) PRIMARY KEY,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "profileId" varchar(10) NOT NULL,
  "stageExecutionRunId" varchar(10) NOT NULL,
  "stageInputHash" varchar(64) NOT NULL,
  "normalisationInputRevision" bigint NOT NULL,
  "inputSignature" varchar(64) NOT NULL,
  "calculationVersion" varchar(64) NOT NULL,
  "status" varchar(20) NOT NULL,
  "summary" jsonb,
  "errorMessage" text,
  "createdBy" varchar(10),
  "startedAt" timestamptz NOT NULL DEFAULT now(),
  "completedAt" timestamptz,
  "createdAt" timestamptz NOT NULL DEFAULT now(),
  "updatedAt" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "tbl_ptrs_payment_normalisation_row" (
  "id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "normalisationResultId" varchar(10) NOT NULL,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "stageRowId" varchar(10) NOT NULL,
  "rowNo" integer NOT NULL,
  "normalisationGroupKey" text,
  "normalisationRole" varchar(30) NOT NULL,
  "documentType" varchar(50),
  "companyCode" text,
  "sourceAccountCode" text,
  "clearingDocument" text,
  "normalisationAmount" numeric NOT NULL,
  "originalObligationAmount" numeric,
  "adjustedObligationAmount" numeric,
  "adjustmentAllocatedAmount" numeric,
  "paymentAllocatedAmount" numeric,
  "outstandingAmount" numeric,
  "unmatchedAmount" numeric,
  "reversalOffsetAmount" numeric,
  "exceptionCode" varchar(64),
  "createdAt" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "tbl_ptrs_payment_normalisation_allocation" (
  "id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "normalisationResultId" varchar(10) NOT NULL,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "invoiceStageRowId" varchar(10) NOT NULL,
  "paymentStageRowId" varchar(10) NOT NULL,
  "paymentSequence" bigint NOT NULL,
  "normalisationGroupKey" text NOT NULL,
  "companyCode" text,
  "sourceAccountCode" text,
  "clearingDocument" text,
  "allocatedAmount" numeric NOT NULL,
  "originalObligationAmount" numeric NOT NULL,
  "adjustedObligationAmount" numeric NOT NULL,
  "adjustmentAllocatedAmount" numeric NOT NULL,
  "settlementPaymentDate" date,
  "sourcePaymentAmount" numeric NOT NULL,
  "obligationBeforePayment" numeric NOT NULL,
  "obligationAfterPayment" numeric NOT NULL,
  "partialPayment" boolean NOT NULL,
  "finalSettlement" boolean NOT NULL,
  "paymentTimeDays" integer,
  "paymentTimeReferenceKind" varchar(40),
  "paymentTimeReferenceDate" date,
  "paymentTimeReferencePolicy" varchar(100),
  "paymentTimeReferenceReason" text,
  "mappingExceptionCode" varchar(64),
  "createdAt" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "tbl_ptrs_payment_normalisation_exception" (
  "id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "normalisationResultId" varchar(10) NOT NULL,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "sourceStageRowId" varchar(10) NOT NULL,
  "reasonCode" varchar(64) NOT NULL,
  "amount" numeric NOT NULL,
  "documentType" varchar(50),
  "createdAt" timestamptz NOT NULL DEFAULT now()
);

-- Local/model-first environments may have created the result table before the
-- migration. Converge that safe shape without replacing governed result data.
ALTER TABLE "tbl_ptrs_payment_normalisation_result"
  ADD COLUMN IF NOT EXISTS "normalisationInputRevision" bigint NOT NULL DEFAULT 0;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = '"tbl_ptrs_payment_normalisation_result"'::regclass
      AND conname = 'ptrs_payment_normalisation_result_ptrs_fk'
  ) THEN
    ALTER TABLE "tbl_ptrs_payment_normalisation_result"
      ADD CONSTRAINT "ptrs_payment_normalisation_result_ptrs_fk"
      FOREIGN KEY ("ptrsId") REFERENCES "tbl_ptrs"("id") ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = '"tbl_ptrs_payment_normalisation_result"'::regclass
      AND conname = 'ptrs_payment_normalisation_result_status_ck'
  ) THEN
    ALTER TABLE "tbl_ptrs_payment_normalisation_result"
      ADD CONSTRAINT "ptrs_payment_normalisation_result_status_ck"
      CHECK ("status" IN ('calculating', 'succeeded', 'failed'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = '"tbl_ptrs_payment_normalisation_result"'::regclass
      AND conname = 'ptrs_payment_normalisation_result_succeeded_ck'
  ) THEN
    ALTER TABLE "tbl_ptrs_payment_normalisation_result"
      ADD CONSTRAINT "ptrs_payment_normalisation_result_succeeded_ck"
      CHECK (
        "status" <> 'succeeded'
        OR ("summary" IS NOT NULL AND "completedAt" IS NOT NULL)
      );
  END IF;
END;
$$;

ALTER TABLE "tbl_ptrs_payment_normalisation_row"
  DROP CONSTRAINT IF EXISTS "ptrs_payment_normalisation_row_result_fk";
ALTER TABLE "tbl_ptrs_payment_normalisation_row"
  ADD CONSTRAINT "ptrs_payment_normalisation_row_result_fk"
  FOREIGN KEY ("normalisationResultId")
  REFERENCES "tbl_ptrs_payment_normalisation_result"("id") ON DELETE CASCADE;

ALTER TABLE "tbl_ptrs_payment_normalisation_allocation"
  DROP CONSTRAINT IF EXISTS "ptrs_payment_normalisation_allocation_result_fk";
ALTER TABLE "tbl_ptrs_payment_normalisation_allocation"
  ADD CONSTRAINT "ptrs_payment_normalisation_allocation_result_fk"
  FOREIGN KEY ("normalisationResultId")
  REFERENCES "tbl_ptrs_payment_normalisation_result"("id") ON DELETE CASCADE;

ALTER TABLE "tbl_ptrs_payment_normalisation_exception"
  DROP CONSTRAINT IF EXISTS "ptrs_payment_normalisation_exception_result_fk";
ALTER TABLE "tbl_ptrs_payment_normalisation_exception"
  ADD CONSTRAINT "ptrs_payment_normalisation_exception_result_fk"
  FOREIGN KEY ("normalisationResultId")
  REFERENCES "tbl_ptrs_payment_normalisation_result"("id") ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_payment_normalisation_result_identity_ux"
  ON "tbl_ptrs_payment_normalisation_result" (
    "customerId", "ptrsId", "inputSignature", "calculationVersion"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_result_scope_created_idx"
  ON "tbl_ptrs_payment_normalisation_result" (
    "customerId", "ptrsId", "createdAt"
  );
CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_payment_normalisation_row_result_stage_ux"
  ON "tbl_ptrs_payment_normalisation_row" (
    "normalisationResultId", "stageRowId"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_row_result_role_idx"
  ON "tbl_ptrs_payment_normalisation_row" (
    "normalisationResultId", "normalisationRole"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_row_result_group_idx"
  ON "tbl_ptrs_payment_normalisation_row" (
    "normalisationResultId", "normalisationGroupKey"
  );
CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_payment_normalisation_allocation_pair_ux"
  ON "tbl_ptrs_payment_normalisation_allocation" (
    "normalisationResultId", "invoiceStageRowId", "paymentStageRowId"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_allocation_result_payment_idx"
  ON "tbl_ptrs_payment_normalisation_allocation" (
    "normalisationResultId", "paymentStageRowId"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_allocation_result_invoice_idx"
  ON "tbl_ptrs_payment_normalisation_allocation" (
    "normalisationResultId", "invoiceStageRowId"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_exception_result_reason_idx"
  ON "tbl_ptrs_payment_normalisation_exception" (
    "normalisationResultId", "reasonCode"
  );
CREATE INDEX IF NOT EXISTS "ptrs_payment_normalisation_exception_result_source_idx"
  ON "tbl_ptrs_payment_normalisation_exception" (
    "normalisationResultId", "sourceStageRowId"
  );

CREATE OR REPLACE FUNCTION "prevent_succeeded_ptrs_normalisation_result_update"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD."status" = 'succeeded' THEN
    RAISE EXCEPTION 'Successful PTRS payment normalisation results are immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS "ptrs_payment_normalisation_result_immutable_trg"
  ON "tbl_ptrs_payment_normalisation_result";
CREATE TRIGGER "ptrs_payment_normalisation_result_immutable_trg"
BEFORE UPDATE ON "tbl_ptrs_payment_normalisation_result"
FOR EACH ROW
EXECUTE FUNCTION "prevent_succeeded_ptrs_normalisation_result_update"();

CREATE OR REPLACE FUNCTION "prevent_succeeded_ptrs_normalisation_child_change"()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM "tbl_ptrs_payment_normalisation_result" result
    WHERE result."id" = OLD."normalisationResultId"
      AND result."status" = 'succeeded'
  ) THEN
    RAISE EXCEPTION 'Successful PTRS payment normalisation result rows are immutable';
  END IF;
  RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS "ptrs_payment_normalisation_row_immutable_trg"
  ON "tbl_ptrs_payment_normalisation_row";
CREATE TRIGGER "ptrs_payment_normalisation_row_immutable_trg"
BEFORE UPDATE OR DELETE ON "tbl_ptrs_payment_normalisation_row"
FOR EACH ROW
EXECUTE FUNCTION "prevent_succeeded_ptrs_normalisation_child_change"();

DROP TRIGGER IF EXISTS "ptrs_payment_normalisation_allocation_immutable_trg"
  ON "tbl_ptrs_payment_normalisation_allocation";
CREATE TRIGGER "ptrs_payment_normalisation_allocation_immutable_trg"
BEFORE UPDATE OR DELETE ON "tbl_ptrs_payment_normalisation_allocation"
FOR EACH ROW
EXECUTE FUNCTION "prevent_succeeded_ptrs_normalisation_child_change"();

DROP TRIGGER IF EXISTS "ptrs_payment_normalisation_exception_immutable_trg"
  ON "tbl_ptrs_payment_normalisation_exception";
CREATE TRIGGER "ptrs_payment_normalisation_exception_immutable_trg"
BEFORE UPDATE OR DELETE ON "tbl_ptrs_payment_normalisation_exception"
FOR EACH ROW
EXECUTE FUNCTION "prevent_succeeded_ptrs_normalisation_child_change"();

ALTER TABLE "tbl_ptrs_payment_normalisation_result" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "tbl_ptrs_payment_normalisation_row" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "tbl_ptrs_payment_normalisation_allocation" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "tbl_ptrs_payment_normalisation_exception" ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "ptrs_payment_normalisation_result_rls_policy"
  ON "tbl_ptrs_payment_normalisation_result";
CREATE POLICY "ptrs_payment_normalisation_result_rls_policy"
  ON "tbl_ptrs_payment_normalisation_result" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

DROP POLICY IF EXISTS "ptrs_payment_normalisation_row_rls_policy"
  ON "tbl_ptrs_payment_normalisation_row";
CREATE POLICY "ptrs_payment_normalisation_row_rls_policy"
  ON "tbl_ptrs_payment_normalisation_row" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

DROP POLICY IF EXISTS "ptrs_payment_normalisation_allocation_rls_policy"
  ON "tbl_ptrs_payment_normalisation_allocation";
CREATE POLICY "ptrs_payment_normalisation_allocation_rls_policy"
  ON "tbl_ptrs_payment_normalisation_allocation" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

DROP POLICY IF EXISTS "ptrs_payment_normalisation_exception_rls_policy"
  ON "tbl_ptrs_payment_normalisation_exception";
CREATE POLICY "ptrs_payment_normalisation_exception_rls_policy"
  ON "tbl_ptrs_payment_normalisation_exception" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);

ALTER TABLE "tbl_ptrs_payment_normalisation_result" FORCE ROW LEVEL SECURITY;
ALTER TABLE "tbl_ptrs_payment_normalisation_row" FORCE ROW LEVEL SECURITY;
ALTER TABLE "tbl_ptrs_payment_normalisation_allocation" FORCE ROW LEVEL SECURITY;
ALTER TABLE "tbl_ptrs_payment_normalisation_exception" FORCE ROW LEVEL SECURITY;

COMMIT;
