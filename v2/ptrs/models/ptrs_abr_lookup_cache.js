const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  return sequelize.define(
    "PtrsAbrLookupCache",
    {
      id: {
        type: DataTypes.STRING(10),
        defaultValue: () => getNanoid(10),
        primaryKey: true,
      },
      abn: {
        type: DataTypes.STRING(11),
        allowNull: false,
      },
      classification: {
        type: DataTypes.STRING(32),
        allowNull: false,
      },
      checkedAt: {
        type: DataTypes.DATE,
        allowNull: false,
      },
      expiresAt: {
        type: DataTypes.DATE,
        allowNull: false,
      },
    },
    {
      tableName: "tbl_ptrs_abr_lookup_cache",
      timestamps: true,
    },
  );
}
