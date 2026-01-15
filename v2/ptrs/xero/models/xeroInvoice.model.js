const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// Cached Xero AP invoice / bill records.
// Used as an intermediate cache for PTRS dataset construction.
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

    xeroTenantId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    xeroInvoiceId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    invoiceNumber: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    contactId: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    invoiceDate: {
      type: DataTypes.DATE,
      allowNull: true,
    },

    dueDate: {
      type: DataTypes.DATE,
      allowNull: true,
    },

    status: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    total: {
      type: DataTypes.DECIMAL(15, 2),
      allowNull: true,
    },

    currency: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    rawPayload: {
      type: DataTypes.JSONB,
      allowNull: false,
    },

    fetchedAt: {
      type: DataTypes.DATE,
      allowNull: false,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsXeroInvoice = sequelize.define("PtrsXeroInvoice", attributes, {
    tableName: "tbl_ptrs_xero_invoice",
    timestamps: true,
    paranoid: true,
    indexes: [
      {
        fields: ["customerId", "xeroTenantId"],
        name: "ix_ptrs_xero_invoice_customer_tenant",
      },
      {
        fields: ["customerId", "xeroTenantId", "xeroInvoiceId"],
        unique: true,
        name: "ux_ptrs_xero_invoice_customer_tenant_invoice",
      },
    ],
  });

  return PtrsXeroInvoice;
}
