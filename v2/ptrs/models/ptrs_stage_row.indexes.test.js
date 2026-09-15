const fs = require("fs");
const path = require("path");
const { Sequelize } = require("sequelize");
const definePtrsStageRow = require("./ptrs_stage_row");

const RETAINED_INDEX_NAMES = [
  "ptrs_stage_row_customer_id_idx",
  "ptrs_stage_row_ptrs_id_idx",
  "ptrs_stage_row_profile_id_idx",
  "ptrs_stage_row_dataset_id_idx",
  "ptrs_stage_row_canonical_revision_idx",
  "ptrs_stage_row_canonical_source_row_idx",
  "ptrs_stage_row_customer_ptrs_idx",
  "ptrs_stage_row_customer_ptrs_profile_idx",
  "ptrs_stage_row_ptrs_dataset_idx",
  "ptrs_stage_row_customer_ptrs_dataset_idx",
  "ptrs_stage_row_customer_ptrs_profile_dataset_rowno_idx",
];

const DUPLICATE_INDEX_NAMES = [
  "tbl_ptrs_stage_row_customer_id",
  "tbl_ptrs_stage_row_customer_id_ptrs_id",
  "tbl_ptrs_stage_row_customer_id_ptrs_id_dataset_id",
  "tbl_ptrs_stage_row_customer_id_ptrs_id_profile_id",
  "tbl_ptrs_stage_row_customer_id_ptrs_id_profile_id_dataset_id_ro",
  "tbl_ptrs_stage_row_dataset_id",
  "tbl_ptrs_stage_row_profile_id",
  "tbl_ptrs_stage_row_ptrs_id",
  "tbl_ptrs_stage_row_ptrs_id_dataset_id",
];

describe("PTRS Stage index governance", () => {
  test("model retains only the explicit current Stage index names", async () => {
    const sequelize = new Sequelize({ dialect: "postgres", logging: false });
    const model = definePtrsStageRow(sequelize);

    expect(model.options.indexes.map((index) => index.name)).toEqual(
      RETAINED_INDEX_NAMES,
    );
    expect(model.options.indexes.every((index) => Boolean(index.name))).toBe(
      true,
    );
    expect(
      model.options.indexes.some((index) =>
        DUPLICATE_INDEX_NAMES.includes(index.name),
      ),
    ).toBe(false);

    await sequelize.close();
  });

  test("migration concurrently removes every confirmed exact duplicate", () => {
    const migration = fs.readFileSync(
      path.join(
        __dirname,
        "../../../db/migrations/20260914_ptrs_stage_duplicate_index_cleanup.sql",
      ),
      "utf8",
    );

    for (const indexName of DUPLICATE_INDEX_NAMES) {
      expect(migration).toContain(
        `DROP INDEX CONCURRENTLY IF EXISTS "${indexName}";`,
      );
    }
    for (const indexName of RETAINED_INDEX_NAMES) {
      expect(migration).not.toContain(`IF EXISTS "${indexName}";`);
    }
    expect(migration).not.toMatch(/\bBEGIN\b|\bCOMMIT\b/);
  });
});
