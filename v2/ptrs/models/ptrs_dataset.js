const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      defaultValue: () => getNanoid(10),
    },

    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // Retained as a display/source namespace. Dataset purpose and concrete id
    // are authoritative for orchestration and joins.
    role: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    purpose: {
      type: DataTypes.STRING(20),
      allowNull: false,
      validate: { isIn: [["transaction", "reference"]] },
    },

    sourceFormat: {
      type: DataTypes.STRING(20),
      allowNull: false,
      validate: { isIn: [["csv", "xlsx", "api"]] },
    },

    adapterType: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    adapterVersion: {
      type: DataTypes.STRING(30),
      allowNull: true,
    },

    referenceKind: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    sourceGroupScope: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },

    // Where this dataset came from (e.g. "xero", "excel", "myob_excel")
    sourceType: {
      type: DataTypes.STRING(30),
      allowNull: true,
    },

    fileName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    // Storage key / path (e.g. S3 key or local path)
    storageRef: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    // Optional: number of parsed rows in the dataset
    rowsCount: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    // Simple lifecycle status for the dataset:
    //   - "uploading"
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

  const PtrsDataset = sequelize.define("PtrsDataset", attributes, {
    tableName: "tbl_ptrs_dataset",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["ptrsId", "purpose"] },
      { fields: ["ptrsId", "purpose", "referenceKind"] },
      { fields: ["sourceType"] },
      { fields: ["customerId", "ptrsId"] },
    ],
  });

  return PtrsDataset;
}
