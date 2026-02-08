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

    // RLS discriminator
    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // PTRS report container
    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // Import run grouping (so multiple import attempts don't mix)
    importRunId: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    // Source system / context
    source: {
      type: DataTypes.STRING(30),
      allowNull: false,
    },
    phase: {
      type: DataTypes.STRING(80),
      allowNull: false,
    },

    // e.g. error | warn
    severity: {
      type: DataTypes.STRING(10),
      allowNull: false,
      defaultValue: "error",
    },

    // HTTP-ish fields
    statusCode: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },
    method: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    url: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    message: {
      type: DataTypes.TEXT,
      allowNull: true,
    },

    // Xero-specific identifiers (nullable so we can store other exception types later)
    xeroTenantId: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    invoiceId: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    // Response / metadata
    responseBody: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
  };

  const PtrsImportException = sequelize.define(
    "PtrsImportException",
    attributes,
    {
      tableName: "tbl_ptrs_import_exception",
      timestamps: true,
      paranoid: true,
    },
  );

  return PtrsImportException;
}
