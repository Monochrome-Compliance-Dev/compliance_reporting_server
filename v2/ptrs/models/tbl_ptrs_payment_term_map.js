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
      type: DataTypes.STRING,
      allowNull: false,
    },

    profileId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    // Exact raw value as it appears in the dataset (e.g. "0010", "NT60").
    raw: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    // Resolved term length in days (exact match mapping).
    transformedDays: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },

    // Optional metadata for audit/debug.
    note: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    createdBy: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    updatedBy: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsPaymentTermMap = sequelize.define(
    "PtrsPaymentTermMap",
    attributes,
    {
      tableName: "tbl_ptrs_payment_term_map",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          unique: true,
          fields: ["customerId", "profileId", "raw"],
          name: "ux_ptrs_payment_term_map_customer_profile_raw",
        },
        {
          fields: ["customerId", "profileId"],
          name: "ix_ptrs_payment_term_map_customer_profile",
        },
      ],
    }
  );

  return PtrsPaymentTermMap;
}
