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

    name: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    description: {
      type: DataTypes.TEXT,
      allowNull: true,
    },

    product: {
      type: DataTypes.STRING(16),
      allowNull: false, // 'ptrs', 'pulse', etc.
    },

    status: {
      type: DataTypes.STRING(32),
      allowNull: false,
      defaultValue: "active",
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

  const CustomerProfile = sequelize.define("CustomerProfile", attributes, {
    tableName: "tbl_customer_profile",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["customerId", "product"] },
      // optional, if you want to optimise name lookup:
      // { fields: ["customerId", "product", "name"] },
    ],
  });

  return CustomerProfile;
}
