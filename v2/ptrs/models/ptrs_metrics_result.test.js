const fs = require("fs");
const path = require("path");

describe("PTRS metrics result persistence", () => {
  const migrationPath = path.join(
    __dirname,
    "../../../db/migrations/20260830_ptrs_metrics_results.sql",
  );

  test("migration converges a Sequelize-created table without dropping it", () => {
    const sql = fs.readFileSync(migrationPath, "utf8");

    expect(sql).toContain(
      'CREATE TABLE IF NOT EXISTS "tbl_ptrs_metrics_result"',
    );
    expect(sql).not.toMatch(/DROP\s+TABLE\s+.*tbl_ptrs_metrics_result/i);
    expect(sql).toContain('ALTER COLUMN "startedAt" SET DEFAULT now()');
    expect(sql).toContain("FROM pg_constraint");
    expect(sql).toContain("conname = 'ptrs_metrics_result_ptrs_fk'");
    expect(sql).toContain("conname = 'ptrs_metrics_result_status_ck'");
    expect(sql).toContain(
      "conname = 'ptrs_metrics_result_succeeded_payload_ck'",
    );
  });

  test("migration enforces tenant isolation and one result per signature", () => {
    const sql = fs.readFileSync(migrationPath, "utf8");

    expect(sql).toContain(
      'CREATE UNIQUE INDEX IF NOT EXISTS "ptrs_metrics_result_signature_ux"',
    );
    expect(sql).toContain(
      "\"customerId\" = current_setting('app.current_customer_id', true)::text",
    );
    expect(sql).toContain(
      'ALTER TABLE "tbl_ptrs_metrics_result" FORCE ROW LEVEL SECURITY',
    );
    expect(sql).toContain(
      'CHECK ("status" <> \'succeeded\' OR "aggregateResult" IS NOT NULL)',
    );
    expect(sql).toContain(
      'ADD COLUMN IF NOT EXISTS "metricsMaterialRevision" bigint',
    );
    expect(sql).toContain(
      'CREATE OR REPLACE FUNCTION "bump_ptrs_metrics_material_revision_from_new"',
    );
    expect(sql).toContain(
      'DROP TRIGGER IF EXISTS "ptrs_stage_row_metrics_revision_insert_trg"',
    );
    expect(sql).toContain(
      'CREATE TRIGGER "ptrs_stage_row_metrics_revision_insert_trg"',
    );
    expect(sql).toContain(
      'CREATE TRIGGER "ptrs_stage_row_metrics_revision_update_trg"',
    );
    expect(sql).toContain(
      'CREATE TRIGGER "ptrs_stage_row_metrics_revision_delete_trg"',
    );
  });

  test("repository startup intentionally uses non-altering Sequelize sync", () => {
    const databaseSource = fs.readFileSync(
      path.join(__dirname, "../../../db/database.js"),
      "utf8",
    );

    expect(databaseSource).toContain("await sequelize.sync();");
    expect(databaseSource).not.toMatch(
      /sequelize\.sync\(\{[^}]*alter\s*:\s*true/s,
    );
  });
});
