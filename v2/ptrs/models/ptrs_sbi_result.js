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

    sbiUploadId: {
      type: DataTypes.STRING(10),
      allowNull: false,
      // FK → tbl_ptrs_sbi_upload.id (declared in /database/index.js)
    },

    // Normalised ABN (digits only)
    abn: {
      type: DataTypes.STRING(20),
      allowNull: false,
    },

    // Raw outcome text from SBI tool
    outcome: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    // Optional year column from the SBI results file
    year: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    // Convenience flag for invalid/unrecognised ABNs
    isValidAbn: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsSbiResult = sequelize.define("PtrsSbiResult", attributes, {
    tableName: "tbl_ptrs_sbi_result",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["sbiUploadId"] },
      { fields: ["abn"] },
      { unique: true, fields: ["sbiUploadId", "abn"] },
    ],
  });

  return PtrsSbiResult;
}
