const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// Cached Xero payment records.
// These represent payment events and will later be expanded into allocation lines.
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

    xeroPaymentId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    invoiceId: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    paymentDate: {
      type: DataTypes.DATE,
      allowNull: true,
    },

    amount: {
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

  const PtrsXeroPayment = sequelize.define("PtrsXeroPayment", attributes, {
    tableName: "tbl_ptrs_xero_payment",
    timestamps: true,
    paranoid: true,
    indexes: [
      {
        fields: ["customerId", "xeroTenantId"],
        name: "ix_ptrs_xero_payment_customer_tenant",
      },
      {
        fields: ["customerId", "xeroTenantId", "xeroPaymentId"],
        unique: true,
        name: "ux_ptrs_xero_payment_customer_tenant_payment",
      },
    ],
  });

  return PtrsXeroPayment;
}
