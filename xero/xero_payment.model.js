const { DataTypes } = require("sequelize");

module.exports = (sequelize) => {
  const XeroPayment = sequelize.define(
    "XeroPayment",
    {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },
      customerId: {
        type: DataTypes.STRING(10),
        allowNull: false,
        references: {
          model: "tbl_customer",
          key: "id",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
      ptrsId: {
        type: DataTypes.STRING(10),
        allowNull: true,
        references: {
          model: "tbl_ptrs",
          key: "id",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
      tenantId: { type: DataTypes.STRING(50), allowNull: false },
      PaymentID: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      Amount: {
        type: DataTypes.DECIMAL(10, 2),
        allowNull: true,
      },
      Date: {
        type: DataTypes.STRING(50),
        allowNull: false,
      },
      Reference: {
        type: DataTypes.STRING(50),
        allowNull: true,
      },
      PaymentType: {
        type: DataTypes.STRING(255),
        allowNull: true,
      },
      IsReconciled: {
        type: DataTypes.BOOLEAN,
        allowNull: true,
        defaultValue: false,
      },
      Status: {
        type: DataTypes.STRING(50),
        allowNull: true,
      },
      Invoice: {
        type: DataTypes.JSONB,
        allowNull: true,
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
      tableName: "xero_payments",
      timestamps: true,
      paranoid: true, // enable soft-deletes via deletedAt
    }
  );

  return XeroPayment;
};
