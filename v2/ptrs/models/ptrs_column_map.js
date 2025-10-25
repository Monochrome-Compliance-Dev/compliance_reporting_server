const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: Column mapping config for a run.
 * Shape (stored mostly as JSONB for flexibility):
 * {
 *   mappings: { "<sourceHeader>": { field: "<canonical>", type: "<type>", fmt?: "<format>", alias?: "<string>" } },
 *   extras:   { "<sourceHeader>": "<alias|null>" }, // passthrough keep/drop + alias
 *   fallbacks:{ "<canonicalField>": ["Alt Header A","Alt Header B", "RUN_DEFAULT:payerEntityName"] },
 *   defaults: { "payerEntityName": "...", "payerEntityAbn": "..." },
 *   joins:    { /* dataset join hints *-/ },
 *   rowRules: [ /* row-level adjustment rules (e.g., negate ET) *-/ ],
 *   profileId:"veolia" | "cosol" | null
 * }
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    runId: { type: DataTypes.STRING(10), allowNull: false },

    // Core mappings
    mappings: { type: DataTypes.JSONB, allowNull: false },

    // New in v2: richer config to support discovery mode and profiles
    extras: { type: DataTypes.JSONB, allowNull: true }, // passthrough headers & aliases
    fallbacks: { type: DataTypes.JSONB, allowNull: true }, // per-canonical fallback chains
    defaults: { type: DataTypes.JSONB, allowNull: true }, // run-level defaults (e.g., payer name/abn)
    joins: { type: DataTypes.JSONB, allowNull: true }, // dataset join hints
    rowRules: { type: DataTypes.JSONB, allowNull: true }, // row-level rules (ET negate etc.)
    profileId: { type: DataTypes.STRING(40), allowNull: true },

    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const PtrsColumnMap = sequelize.define("ptrs_column_map", attributes, {
    tableName: "tbl_ptrs_column_map",
    timestamps: true,
    paranoid: false,
    indexes: [
      { name: "idx_ptrs_column_map_upload", fields: ["runId"] },
      { name: "idx_ptrs_column_map_customer", fields: ["customerId"] },
    ],
  });

  return PtrsColumnMap;
}
