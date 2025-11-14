const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      defaultValue: async () => await getNanoid()(10),
    },

    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // What role this dataset plays in the PTRS workflow:
    //   - "main"           -> primary PTRS CSV
    //   - "vendor_master"  -> vendor master data
    //   - "entity_master"  -> entity list
    //   - "other_*"        -> any additional supporting datasets
    role: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    fileName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    // Storage key / path (e.g. S3 key or local path)
    storageKey: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    // Optional: number of parsed rows in the dataset
    rowsCount: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    // Simple lifecycle status for the dataset:
    //   - "uploaded"
    //   - "parsed"
    //   - "failed"
    status: {
      type: DataTypes.STRING(30),
      allowNull: false,
      defaultValue: "uploaded",
    },

    // Free-form JSON for stats, first-row samples, inferred types, etc.
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsDataset = sequelize.define("ptrs_dataset", attributes, {
    tableName: "tbl_ptrs_dataset",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["ptrsId", "role"] },
    ],
  });

  return PtrsDataset;
}
