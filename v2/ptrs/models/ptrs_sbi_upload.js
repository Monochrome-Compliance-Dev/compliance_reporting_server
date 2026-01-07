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

    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
      // FK → tbl_ptrs.id (declared in /database/index.js)
    },

    // Original filename provided by the user
    fileName: {
      type: DataTypes.STRING(512),
      allowNull: true,
    },

    // sha256 hash of the raw uploaded file bytes
    fileHash: {
      type: DataTypes.STRING(64),
      allowNull: false,
    },

    // Count of rows in the raw CSV (including header if present)
    rawRowCount: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    // Count of parsed ABNs (deduped)
    parsedAbnCount: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    // APPLIED | APPLIED_WITH_WARNINGS | BLOCKED
    status: {
      type: DataTypes.STRING(40),
      allowNull: false,
      defaultValue: "APPLIED",
    },

    // Summary counts + warnings + reconciliation notes
    summary: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    uploadedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    appliedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsSbiUpload = sequelize.define("PtrsSbiUpload", attributes, {
    tableName: "tbl_ptrs_sbi_upload",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["ptrsId"] },
      { fields: ["customerId", "ptrsId"] },
      { fields: ["fileHash"] },
      { fields: ["status"] },
    ],
  });

  return PtrsSbiUpload;
}
