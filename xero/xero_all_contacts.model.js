const { DataTypes } = require("sequelize");

// Scrap-and-dump store of ALL Xero contacts as received (duplicates allowed)
// Mirrors the factory pattern used in xero_contact.model.js
const XeroAllContacts = (sequelize) => {
  return sequelize.define(
    "XeroAllContacts",
    {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },

      // tenancy & lineage
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
      tenantId: { type: DataTypes.STRING(64), allowNull: false },

      // --- Xero Contact fields (as per documentation) ---
      ContactID: { type: DataTypes.STRING(255), allowNull: true }, // not unique on purpose
      ContactNumber: { type: DataTypes.STRING(255), allowNull: true },
      AccountNumber: { type: DataTypes.STRING(255), allowNull: true },
      ContactStatus: { type: DataTypes.STRING(64), allowNull: true },

      Name: { type: DataTypes.STRING, allowNull: true },
      FirstName: { type: DataTypes.STRING, allowNull: true },
      LastName: { type: DataTypes.STRING, allowNull: true },
      EmailAddress: { type: DataTypes.STRING, allowNull: true },
      SkypeUserName: { type: DataTypes.STRING, allowNull: true }, // deprecated in Xero UI, retained for completeness

      BankAccountDetails: { type: DataTypes.STRING, allowNull: true },
      CompanyNumber: { type: DataTypes.STRING(50), allowNull: true },
      TaxNumber: { type: DataTypes.STRING, allowNull: true },

      AccountsReceivableTaxType: {
        type: DataTypes.STRING(64),
        allowNull: true,
      },
      AccountsPayableTaxType: { type: DataTypes.STRING(64), allowNull: true },

      Addresses: { type: DataTypes.JSONB, allowNull: true },
      Phones: { type: DataTypes.JSONB, allowNull: true },

      IsSupplier: { type: DataTypes.BOOLEAN, allowNull: true },
      IsCustomer: { type: DataTypes.BOOLEAN, allowNull: true },

      DefaultCurrency: { type: DataTypes.STRING(10), allowNull: true },
      UpdatedDateUTC: { type: DataTypes.DATE, allowNull: true },

      // audit/meta
      source: {
        type: DataTypes.STRING(50),
        allowNull: false,
        defaultValue: "Xero",
      },
      createdBy: { type: DataTypes.STRING(50), allowNull: false },
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
      tableName: "xero_all_contacts",
      timestamps: true,
      paranoid: true, // enable soft-deletes via deletedAt
      updatedAt: "updatedAt",
      createdAt: "createdAt",
      indexes: [
        { fields: ["customerId"] },
        { fields: ["tenantId"] },
        { fields: ["customerId", "tenantId"] },
        { fields: ["ContactID"] },
        { fields: ["UpdatedDateUTC"] },
      ],
    }
  );
};

module.exports = XeroAllContacts;
