BEGIN;

DROP TABLE IF EXISTS "tbl_ptrs_canonical_source_row" CASCADE;
DROP TABLE IF EXISTS "tbl_ptrs_canonical_revision" CASCADE;

CREATE TABLE "tbl_ptrs_canonical_revision" (
  "id" varchar(10) PRIMARY KEY,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "datasetId" varchar(10) NOT NULL,
  "adapterType" varchar(50) NOT NULL,
  "adapterVersion" varchar(30),
  "sourceGroupScope" varchar(100),
  "canonicalVersion" varchar(30) NOT NULL,
  "semanticKind" varchar(30) NOT NULL,
  "sourceSignature" varchar(64) NOT NULL,
  "mappingSignature" varchar(64) NOT NULL,
  "enrichmentSignature" varchar(64) NOT NULL,
  "materialSignature" varchar(64) NOT NULL,
  "inputSnapshot" jsonb NOT NULL,
  "status" varchar(20) NOT NULL,
  "rowCount" integer,
  "completedAt" timestamptz,
  "failure" jsonb,
  "createdBy" varchar(10),
  "createdAt" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "ptrs_canonical_revision_dataset_fk"
    FOREIGN KEY ("datasetId") REFERENCES "tbl_ptrs_dataset"("id") ON DELETE CASCADE,
  CONSTRAINT "ptrs_canonical_revision_status_ck"
    CHECK ("status" IN ('building', 'succeeded', 'failed')),
  CONSTRAINT "ptrs_canonical_revision_semantic_kind_ck"
    CHECK ("semanticKind" IN ('accounting_event', 'direct_payment'))
);

CREATE INDEX "ptrs_canonical_revision_scope_idx"
  ON "tbl_ptrs_canonical_revision" ("customerId", "ptrsId", "datasetId", "status");
CREATE INDEX "ptrs_canonical_revision_material_idx"
  ON "tbl_ptrs_canonical_revision" ("materialSignature");
CREATE UNIQUE INDEX "ptrs_canonical_revision_success_material_ux"
  ON "tbl_ptrs_canonical_revision" ("customerId", "ptrsId", "datasetId", "materialSignature")
  WHERE "status" = 'succeeded';

CREATE TABLE "tbl_ptrs_canonical_source_row" (
  "id" varchar(10) PRIMARY KEY,
  "customerId" varchar(10) NOT NULL,
  "ptrsId" varchar(10) NOT NULL,
  "canonicalRevisionId" varchar(10) NOT NULL,
  "datasetId" varchar(10) NOT NULL,
  "sourceRawRowId" varchar(10),
  "sourceRowNo" integer NOT NULL,
  "sourceGroupScope" varchar(100),
  "adapterType" varchar(50) NOT NULL,
  "adapterVersion" varchar(30),
  "semanticKind" varchar(30) NOT NULL,
  "payerEntityName" varchar(255),
  "payerEntityAbn" varchar(255),
  "payeeEntityName" varchar(255),
  "payeeEntityAbn" varchar(255),
  "payeeEntityAbnValid" boolean,
  "invoiceReferenceNumber" varchar(255),
  "sourceAccountCode" varchar(255),
  "description" text,
  "documentType" varchar(255),
  "documentCurrency" varchar(255),
  "clearingDocument" varchar(255),
  "reconciliationStatus" varchar(255),
  "sourceUser" varchar(255),
  "paymentAmount" numeric(18,2),
  "paymentDate" date,
  "invoiceIssueDate" date,
  "invoiceReceiptDate" date,
  "invoiceDueDate" date,
  "invoiceCreatedDate" date,
  "entryDate" date,
  "paymentTermRaw" varchar(255),
  "data" jsonb NOT NULL,
  "provenance" jsonb NOT NULL,
  "createdAt" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "ptrs_canonical_source_row_revision_fk"
    FOREIGN KEY ("canonicalRevisionId") REFERENCES "tbl_ptrs_canonical_revision"("id") ON DELETE CASCADE,
  CONSTRAINT "ptrs_canonical_source_row_dataset_fk"
    FOREIGN KEY ("datasetId") REFERENCES "tbl_ptrs_dataset"("id") ON DELETE CASCADE,
  CONSTRAINT "ptrs_canonical_source_row_raw_fk"
    FOREIGN KEY ("sourceRawRowId") REFERENCES "tbl_ptrs_import_raw"("id") ON DELETE SET NULL,
  CONSTRAINT "ptrs_canonical_source_row_semantic_kind_ck"
    CHECK ("semanticKind" IN ('accounting_event', 'direct_payment')),
  CONSTRAINT "ptrs_canonical_source_row_source_row_ux"
    UNIQUE ("customerId", "canonicalRevisionId", "sourceRowNo")
);

CREATE INDEX "ptrs_canonical_source_row_scope_idx"
  ON "tbl_ptrs_canonical_source_row" ("customerId", "ptrsId", "datasetId");
CREATE INDEX "ptrs_canonical_source_row_order_idx"
  ON "tbl_ptrs_canonical_source_row" ("canonicalRevisionId", "sourceRowNo", "id");

ALTER TABLE "tbl_ptrs_canonical_revision" ENABLE ROW LEVEL SECURITY;
CREATE POLICY "ptrs_canonical_revision_rls_policy"
  ON "tbl_ptrs_canonical_revision" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE "tbl_ptrs_canonical_revision" FORCE ROW LEVEL SECURITY;

ALTER TABLE "tbl_ptrs_canonical_source_row" ENABLE ROW LEVEL SECURITY;
CREATE POLICY "ptrs_canonical_source_row_rls_policy"
  ON "tbl_ptrs_canonical_source_row" FOR ALL
  USING ("customerId" = current_setting('app.current_customer_id', true)::text)
  WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
ALTER TABLE "tbl_ptrs_canonical_source_row" FORCE ROW LEVEL SECURITY;

-- Historical Stage/mapped rows cannot be assigned trustworthy immutable lineage.
-- Require an explicit canonical and Stage rebuild after this migration.
TRUNCATE TABLE "tbl_ptrs_stage_row";

ALTER TABLE "tbl_ptrs_stage_row"
  ADD COLUMN "canonicalRevisionId" varchar(10) NOT NULL,
  ADD COLUMN "canonicalSourceRowId" varchar(10) NOT NULL,
  ADD COLUMN "sourceRawRowId" varchar(10),
  ADD COLUMN "sourceRowNo" integer NOT NULL,
  ADD COLUMN "adapterType" varchar(50) NOT NULL,
  ADD COLUMN "adapterVersion" varchar(30),
  ADD COLUMN "sourceGroupScope" varchar(100),
  ADD COLUMN "semanticKind" varchar(30) NOT NULL,
  ADD CONSTRAINT "ptrs_stage_row_canonical_revision_fk"
    FOREIGN KEY ("canonicalRevisionId") REFERENCES "tbl_ptrs_canonical_revision"("id"),
  ADD CONSTRAINT "ptrs_stage_row_canonical_source_row_fk"
    FOREIGN KEY ("canonicalSourceRowId") REFERENCES "tbl_ptrs_canonical_source_row"("id"),
  ADD CONSTRAINT "ptrs_stage_row_semantic_kind_ck"
    CHECK ("semanticKind" IN ('accounting_event', 'direct_payment'));

CREATE INDEX "ptrs_stage_row_canonical_revision_idx"
  ON "tbl_ptrs_stage_row" ("canonicalRevisionId");
CREATE INDEX "ptrs_stage_row_canonical_source_row_idx"
  ON "tbl_ptrs_stage_row" ("canonicalSourceRowId");

DROP TABLE IF EXISTS "tbl_ptrs_mapped_row";

COMMIT;
