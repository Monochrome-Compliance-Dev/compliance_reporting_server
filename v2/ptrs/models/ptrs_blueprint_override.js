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

    // Optional profile-scoped override, e.g. "veolia".
    // If null, treat as "default for this customer".
    profileId: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    // Partial blueprint payload – same shape as base/profile JSON
    json: {
      type: DataTypes.JSONB,
      allowNull: false,
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

  const PtrsBlueprintOverride = sequelize.define(
    "PtrsBlueprintOverride",
    attributes,
    {
      tableName: "tbl_ptrs_blueprint_override",
      timestamps: true,
      paranoid: true,
      indexes: [
        { fields: ["customerId"] },
        { fields: ["customerId", "profileId"] },
      ],
    }
  );

  return PtrsBlueprintOverride;
}
