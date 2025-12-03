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
      allowNull: true,
      // FK → tbl_ptrs.id (declared in /database/index.js)
    },

    // Scope of the ruleset: "row" | "crossRow" | "both"
    scope: {
      type: DataTypes.STRING(20),
      allowNull: false,
      defaultValue: "both",
    },

    name: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    description: {
      type: DataTypes.TEXT,
      allowNull: true,
    },

    isDefaultForProfile: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: false,
    },

    // JSONB definition holding rowRules, crossRowRules, etc.
    definition: {
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

  const PtrsRuleset = sequelize.define("PtrsRuleset", attributes, {
    tableName: "tbl_ptrs_ruleset",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["ptrsId"] },
      { fields: ["customerId", "profileId"] },
    ],
  });

  return PtrsRuleset;
}
