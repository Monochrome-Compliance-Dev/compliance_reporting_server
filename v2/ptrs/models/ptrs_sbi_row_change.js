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

    // The candidate payment row identifier that was changed
    paymentRowId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    supplierAbn: {
      type: DataTypes.STRING(20),
      allowNull: true,
    },

    beforeIsSmallBusiness: {
      type: DataTypes.BOOLEAN,
      allowNull: true,
    },

    afterIsSmallBusiness: {
      type: DataTypes.BOOLEAN,
      allowNull: true,
    },

    // Optional: store the raw SBI outcome string that drove the change
    outcome: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    changedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    changedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsSbiRowChange = sequelize.define("PtrsSbiRowChange", attributes, {
    tableName: "tbl_ptrs_sbi_row_change",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["sbiUploadId"] },
      { fields: ["paymentRowId"] },
      { fields: ["supplierAbn"] },
      { fields: ["customerId", "ptrsId"] },
    ],
  });

  return PtrsSbiRowChange;
}
