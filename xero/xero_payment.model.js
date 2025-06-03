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
      Status: {
        type: DataTypes.STRING(50),
        allowNull: true,
      },
      Invoice: {
        type: DataTypes.JSONB,
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
      tableName: "xero_payments",
      timestamps: true,
    }
  );

  return XeroPayment;
};
