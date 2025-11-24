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
      // FK → tbl_ptrs.ptrsId (declared in /database/index.js)
    },

    // The actual column mapping the FE sends down
    mappings: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    // Joins between main/supporting datasets (defined in TablesAndJoinsPanel)
    joins: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    // Explicit default values for mapped fields (e.g. default doc types, ABN source)
    defaults: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    // Fallback behaviour when a primary mapping is missing or invalid
    fallbacks: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    // Advanced metadata + rule-related config (e.g. cross-row rules)
    extras: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    profileId: {
      type: DataTypes.STRING(10),
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

  const PtrsColumnMap = sequelize.define("PtrsColumnMap", attributes, {
    tableName: "tbl_ptrs_column_map",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["ptrsId", "customerId"] },
    ],
  });

  return PtrsColumnMap;
}
