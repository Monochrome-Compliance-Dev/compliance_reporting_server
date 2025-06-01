const { DataTypes } = require("sequelize");
const sequelize = require("../db"); // Adjust path as per your project structure

const XeroOrganisation = sequelize.define(
  "XeroOrganisation",
  {
    clientId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    organisationId: {
      type: DataTypes.STRING,
      allowNull: false,
      unique: true,
    },
    organisationName: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    organisationLegalName: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    organisationAbn: {
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
    tableName: "xero_organisations",
    timestamps: true,
  }
);

module.exports = XeroOrganisation;
