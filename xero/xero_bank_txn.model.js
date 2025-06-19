const { DataTypes } = require("sequelize");

const XeroBankTxn = (sequelize) => {
  return sequelize.define(
    "XeroBankTxn",
    {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },
      clientId: {
        type: DataTypes.STRING(10),
        allowNull: false,
        references: {
          model: "tbl_client",
          key: "id",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
      reportId: {
        type: DataTypes.STRING(10),
        allowNull: true,
        references: {
          model: "tbl_report",
          key: "id",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
      Type: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      Contact: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
      LineItems: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
      BankAccount: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
      LineAmountTypes: {
        type: DataTypes.STRING(50),
        allowNull: true,
      },
      SubTotal: {
        type: DataTypes.DECIMAL(18, 2),
        allowNull: true,
      },
      TotalTax: {
        type: DataTypes.DECIMAL(18, 2),
        allowNull: true,
      },
      Total: {
        type: DataTypes.DECIMAL(18, 2),
        allowNull: true,
      },
      CurrencyCode: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
      BankTransactionID: {
        type: DataTypes.UUID,
        allowNull: false,
      },
      IsReconciled: {
        type: DataTypes.BOOLEAN,
        allowNull: true,
      },
      // Not included in the current paginated response, contrary to the Xero docs: https://developer.xero.com/documentation/api/accounting/banktransactions
      // Url: {
      //   type: DataTypes.STRING(255),
      //   allowNull: true,
      // },
      // Reference: {
      //   type: DataTypes.STRING(255),
      //   allowNull: true,
      // },
      Status: {
        type: DataTypes.STRING(20),
        allowNull: true,
      },
      Date: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      tenantId: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      source: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      createdBy: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      createdAt: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      updatedAt: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
    },
    {
      tableName: "xero_bank_txn",
      timestamps: true,
      updatedAt: "updatedAt",
      createdAt: "createdAt",
    }
  );
};

module.exports = XeroBankTxn;
