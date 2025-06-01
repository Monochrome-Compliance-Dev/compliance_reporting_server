const { DataTypes } = require("sequelize");

const XeroInvoice = (sequelize) => {
  return sequelize.define(
    "XeroInvoice",
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
      invoiceReferenceNumber: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      invoiceIssueDate: {
        type: DataTypes.DATEONLY,
        allowNull: true,
      },
      invoiceDueDate: {
        type: DataTypes.DATEONLY,
        allowNull: true,
      },
      invoiceAmount: {
        type: DataTypes.DECIMAL(15, 2),
        allowNull: true,
      },
      invoicePaymentTerms: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      payeeEntityName: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      payeeEntityAbn: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      payeeEntityAcnArbn: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      paymentAmount: {
        type: DataTypes.DECIMAL(15, 2),
        allowNull: true,
      },
      paymentDate: {
        type: DataTypes.DATEONLY,
        allowNull: true,
      },
      description: {
        type: DataTypes.TEXT,
        allowNull: true,
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
      tableName: "xero_invoices",
      timestamps: true,
    }
  );
};

module.exports = XeroInvoice;
