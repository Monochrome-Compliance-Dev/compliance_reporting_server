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

    customerProfileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    label: {
      type: DataTypes.STRING(255),
      allowNull: false,
      // Example: "Veolia – Default Profile"
    },

    description: {
      type: DataTypes.TEXT,
      allowNull: true,
    },

    // JSON blob for profile-level defaults:
    //   - standard field overrides
    //   - joining rules
    //   - any profile-specific transformations
    //   - customer-specific metadata
    meta: {
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

  const PtrsProfile = sequelize.define("PtrsProfile", attributes, {
    tableName: "tbl_ptrs_profile",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["customerId", "label", "customerProfileId"] },
    ],
  });

  return PtrsProfile;
}
