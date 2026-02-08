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
    field: {
      type: DataTypes.STRING(32),
      allowNull: false,
    },
    term: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },
    matchType: {
      type: DataTypes.STRING(16),
      allowNull: false,
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

  return sequelize.define("PtrsExclusionKeywordCustomerRef", attributes, {
    tableName: "tbl_ptrs_exclusion_keyword_customer_ref",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["customerId", "profileId"] },
      { fields: ["field"] },
      { fields: ["matchType"] },
    ],
  });
}
