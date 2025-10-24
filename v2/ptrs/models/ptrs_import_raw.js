const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * New World PTRS staging model.
 * - Mirrors your nanoid(10) pattern.
 * - Stores each raw CSV row as JSONB (`data`) with optional `errors`.
 * - Scoped by `customerId` and grouped via `runId` (string(10) to match your ID scheme).
 * - Timestamps enabled, no soft deletes (we want immutable ingest history).
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    // Foreign key for Customer scoping (RLS)
    customerId: { type: DataTypes.STRING(10), allowNull: false },

    // Parent upload run (v2 upload model will own this)
    runId: { type: DataTypes.STRING(10), allowNull: false },

    // Original line number from the CSV (1-based)
    rowNo: { type: DataTypes.INTEGER, allowNull: false },

    // Full raw row as JSONB (schema-agnostic)
    data: { type: DataTypes.JSONB, allowNull: false },

    // Optional parse/validation errors captured at ingest time
    errors: { type: DataTypes.JSONB, allowNull: true },
  };

  const PtrsImportRaw = sequelize.define("ptrs_import_raw", attributes, {
    tableName: "tbl_ptrs_import_raw",
    timestamps: true,
    paranoid: false, // keep ingest immutable; if needed, handle logical hide via queries
    indexes: [
      { name: "idx_ptrs_import_raw_upload_row", fields: ["runId", "rowNo"] },
      // Note: JSONB GIN index should be created via SQL migration; Sequelize can't express GIN here.
    ],
  });

  return PtrsImportRaw;
}
