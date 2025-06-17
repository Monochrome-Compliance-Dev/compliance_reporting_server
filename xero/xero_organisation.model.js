const { DataTypes } = require("sequelize");

const XeroOrganisation = (sequelize) => {
  return sequelize.define(
    "XeroOrganisation",
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
      OrganisationID: {
        type: DataTypes.STRING,
        allowNull: false,
        unique: true,
      },
      Name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      LegalName: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      RegistrationNumber: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      TaxNumber: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      PaymentTerms: {
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
      tableName: "xero_organisations",
      schema: "public",
      timestamps: true,
    }
  );
};

module.exports = XeroOrganisation;
