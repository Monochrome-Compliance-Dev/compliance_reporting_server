const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => getNanoid(10),
      primaryKey: true,
    },
    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    counterpartyAbn: {
      type: DataTypes.STRING(14),
      allowNull: true,
    },
    counterpartyName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },
    accountCodePattern: {
      type: DataTypes.STRING(64),
      allowNull: true,
    },
    notes: {
      type: DataTypes.TEXT,
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
  };

  return sequelize.define("PtrsIntraCompanyRef", attributes, {
    tableName: "tbl_ptrs_intra_company_ref",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["customerId", "profileId"] },
      { fields: ["counterpartyAbn"] },
      { fields: ["counterpartyName"] },
    ],
  });
}
