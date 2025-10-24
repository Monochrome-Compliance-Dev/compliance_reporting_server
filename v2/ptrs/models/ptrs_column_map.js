const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: Column mapping from source CSV headers to logical fields/types.
 * Stored as JSONB for flexibility: { "<sourceHeader>": { field: "<logical>", type: "<type>", fmt?: "<format>" } }
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
    mappings: { type: DataTypes.JSONB, allowNull: false },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const PtrsColumnMap = sequelize.define("ptrs_column_map", attributes, {
    tableName: "tbl_ptrs_column_map",
    timestamps: true,
    paranoid: false,
    indexes: [{ name: "idx_ptrs_column_map_upload", fields: ["runId"] }],
  });

  return PtrsColumnMap;
}
