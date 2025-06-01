const { DataTypes } = require("sequelize");

module.exports = (sequelize) => {
  const XeroInvoice = sequelize.define(
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
          key: "clientId",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
      invoiceId: {
        type: DataTypes.STRING,
        allowNull: false,
        unique: true,
      },
      invoiceNumber: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      invoiceDate: {
        type: DataTypes.DATEONLY,
        allowNull: false,
      },
      dueDate: {
        type: DataTypes.DATEONLY,
        allowNull: true,
      },
      totalAmount: {
        type: DataTypes.DECIMAL(18, 2),
        allowNull: false,
      },
      contactName: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      contactEmail: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      status: {
        type: DataTypes.STRING,
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

  return XeroInvoice;
};
