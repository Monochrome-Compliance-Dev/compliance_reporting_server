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
    abn: {
      type: DataTypes.STRING(14),
      allowNull: false,
    },
    name: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },
    category: {
      type: DataTypes.STRING(64),
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

  return sequelize.define("PtrsGovEntityRef", attributes, {
    tableName: "tbl_ptrs_gov_entity_ref",
    timestamps: true,
    paranoid: true,
    indexes: [{ fields: ["abn"] }, { fields: ["name"] }],
  });
}
