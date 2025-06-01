const { DataTypes } = require("sequelize");
const sequelize = require("../db/sequelize"); // adjust path if needed

const XeroPayment = sequelize.define(
  "XeroPayment",
  {
    clientId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    paymentId: {
      type: DataTypes.STRING,
      allowNull: false,
      unique: true,
    },
    paymentAmount: {
      type: DataTypes.DECIMAL(18, 2),
      allowNull: false,
    },
    paymentDate: {
      type: DataTypes.DATEONLY,
      allowNull: false,
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
    description: {
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
    tableName: "xero_payments",
    timestamps: true,
  }
);

module.exports = XeroPayment;
