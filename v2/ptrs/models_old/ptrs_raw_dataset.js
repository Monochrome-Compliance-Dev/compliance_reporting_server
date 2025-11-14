const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: Raw dataset attached to a run (e.g., vendor master, terms changes, entity structure).
 * Stores one record per uploaded auxiliary file; file bytes are stored externally (e.g., S3/local) and referenced by storageRef.
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

    // Role tags how this dataset will be used in joins: transactions|vendorMaster|termsChanges|entityStructure|other
    role: { type: DataTypes.STRING(40), allowNull: false },

    // Friendly name shown in UI (e.g., original file name without path)
    sourceName: { type: DataTypes.STRING(255), allowNull: true },

    // Original upload metadata
    fileName: { type: DataTypes.STRING(255), allowNull: true },
    fileSize: { type: DataTypes.INTEGER, allowNull: true },
    mimeType: { type: DataTypes.STRING(100), allowNull: true },

    // Where the bytes live (e.g., local path or S3 key)
    storageRef: { type: DataTypes.STRING(512), allowNull: true },

    // Free-form metadata (e.g., header list, detected sheet name, row count)
    meta: { type: DataTypes.JSONB, allowNull: true },

    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const PtrsRawDataset = sequelize.define("ptrs_raw_dataset", attributes, {
    tableName: "tbl_ptrs_raw_dataset",
    timestamps: true,
    paranoid: false,
    indexes: [
      { name: "idx_ptrs_raw_dataset_run", fields: ["runId"] },
      { name: "idx_ptrs_raw_dataset_customer", fields: ["customerId"] },
      { name: "idx_ptrs_raw_dataset_role", fields: ["role"] },
    ],
  });

  return PtrsRawDataset;
}
