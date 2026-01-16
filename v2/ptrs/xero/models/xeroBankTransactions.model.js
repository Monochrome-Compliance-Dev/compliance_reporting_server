const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// Cached Xero bank transaction records.
// Raw snapshots for parity with v1 and for future reconciliation/allocation logic.
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

    xeroBankTransactionId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    // Optional convenience fields for filtering/debugging.
    bankTransactionType: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    bankTransactionDate: {
      type: DataTypes.DATE,
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

  const PtrsXeroBankTransaction = sequelize.define(
    "PtrsXeroBankTransaction",
    attributes,
    {
      tableName: "tbl_ptrs_xero_bank_transaction",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          fields: ["customerId", "xeroTenantId"],
          name: "ix_ptrs_xero_bank_tx_customer_tenant",
        },
        {
          fields: ["customerId", "xeroTenantId", "xeroBankTransactionId"],
          unique: true,
          name: "ux_ptrs_xero_bank_tx_customer_tenant_tx",
        },
      ],
    }
  );

  return PtrsXeroBankTransaction;
}
