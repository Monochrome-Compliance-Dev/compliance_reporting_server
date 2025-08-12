const { DataTypes } = require("sequelize");

const XeroContact = (sequelize) => {
  return sequelize.define(
    "XeroContact",
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
      ptrsId: {
        type: DataTypes.STRING(10),
        allowNull: true,
        references: {
          model: "tbl_report",
          key: "id",
        },
        onUpdate: "CASCADE",
        onDelete: "CASCADE",
      },
      ContactID: {
        type: DataTypes.STRING(255),
        allowNull: false,
        unique: true,
      },
      Name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      CompanyNumber: {
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
      tableName: "xero_contacts",
      timestamps: true,
      updatedAt: "updatedAt",
      createdAt: "createdAt",
    }
  );
};

module.exports = XeroContact;
