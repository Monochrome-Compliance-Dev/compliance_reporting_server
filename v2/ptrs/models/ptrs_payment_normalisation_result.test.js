const fs = require("fs");
const path = require("path");

describe("PTRS payment normalisation result persistence", () => {
  const migrationPath = path.join(
    __dirname,
    "../../../db/migrations/20260909_ptrs_payment_normalisation_results.sql",
  );

  test("creates the governed typed result, row, allocation and exception relations", () => {
    const sql = fs.readFileSync(migrationPath, "utf8");

    for (const table of [
      "tbl_ptrs_payment_normalisation_result",
      "tbl_ptrs_payment_normalisation_row",
      "tbl_ptrs_payment_normalisation_allocation",
      "tbl_ptrs_payment_normalisation_exception",
    ]) {
      expect(sql).toContain(`CREATE TABLE IF NOT EXISTS "${table}"`);
      expect(sql).toContain(`ALTER TABLE "${table}" FORCE ROW LEVEL SECURITY`);
    }
    expect(sql).not.toMatch(/DROP\s+TABLE/i);
    expect(sql).toContain('"normalisationInputRevision" bigint NOT NULL');
    expect(sql).toContain('"allocatedAmount" numeric NOT NULL');
    expect(sql).toContain('"paymentTimeReferenceKind" varchar(40)');
    expect(sql).not.toContain('"data" jsonb');
    expect(sql).not.toContain('"meta" jsonb');
  });

  test("enforces identity, successful-result integrity and immutability", () => {
    const sql = fs.readFileSync(migrationPath, "utf8");

    expect(sql).toContain(
      'CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_payment_normalisation_result_identity_ux"',
    );
    expect(sql).toContain(
      'CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_payment_normalisation_row_result_stage_ux"',
    );
    expect(sql).toContain(
      'CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_payment_normalisation_allocation_pair_ux"',
    );
    expect(sql).toContain(
      'CREATE OR REPLACE FUNCTION "prevent_succeeded_ptrs_normalisation_result_update"',
    );
    expect(sql).toContain(
      'CREATE OR REPLACE FUNCTION "prevent_succeeded_ptrs_normalisation_child_change"',
    );
    expect(sql).toContain(
      'CHECK (\n        "status" <> \'succeeded\'\n        OR ("summary" IS NOT NULL AND "completedAt" IS NOT NULL)',
    );
  });

  test("tracks only financial and Payment Time inputs in the invalidation trigger", () => {
    const sql = fs.readFileSync(migrationPath, "utf8");

    expect(sql).toContain('ADD COLUMN IF NOT EXISTS "normalisationInputRevision"');
    expect(sql).toContain('new_row."paymentAmount"');
    expect(sql).toContain('new_row."paymentDate"');
    expect(sql).toContain('new_row."invoiceIssueDate"');
    expect(sql).toContain('new_row."invoiceReceiptDate"');
    expect(sql).toContain("new_row.\"data\"->'company_code'");
    expect(sql).toContain("new_row.\"data\"->'rcti'");
    expect(sql).not.toContain("new_row.\"data\"->'is_small_business'");
    expect(sql).not.toContain("new_row.\"meta\"->'paymentNormalisation'");
  });
});
