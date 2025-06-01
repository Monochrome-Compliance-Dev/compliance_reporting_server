const { DataTypes } = require("sequelize");
const sequelize = require("../db/sequelize"); // Adjust path if needed

const XeroContact = sequelize.define(
  "XeroContact",
  {
    clientId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    contactId: {
      type: DataTypes.STRING,
      allowNull: false,
      unique: true,
    },
    contactName: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    contactAbn: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    contactAcnArbn: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    paymentTerms: {
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
    tableName: "xero_contacts",
    timestamps: true,
    updatedAt: "updatedAt",
    createdAt: "createdAt",
  }
);

module.exports = XeroContact;
