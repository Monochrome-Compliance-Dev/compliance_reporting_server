const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// Tracks effective-dated payment term changes sourced from external systems (e.g., Ariba).
// Intended for SQL-first application (UPDATE ... FROM ...) to avoid heavy per-row JS processing.
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

    // When the change was recorded in the source system.
    changedAt: {
      type: DataTypes.DATE,
      allowNull: false,
    },

    supplier: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    changedBy: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    fieldName: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    companyCode: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    purchOrganisation: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    // Raw term code values as captured in the spreadsheet (exact values).
    newRaw: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    oldRaw: {
      type: DataTypes.STRING,
      allowNull: true,
    },

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

  const PtrsPaymentTermChange = sequelize.define(
    "PtrsPaymentTermChange",
    attributes,
    {
      tableName: "tbl_ptrs_payment_term_change",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          fields: ["customerId", "profileId", "companyCode"],
          name: "ix_ptrs_payment_term_change_customer_profile_company",
        },
        {
          fields: ["customerId", "profileId", "companyCode", "changedAt"],
          name: "ix_ptrs_payment_term_change_customer_profile_company_changedAt",
        },
      ],
    }
  );

  return PtrsPaymentTermChange;
}
