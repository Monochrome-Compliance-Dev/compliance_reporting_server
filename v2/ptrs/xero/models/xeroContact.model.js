const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// Cached Xero contact records to avoid repeated API calls and rate-limit issues.
// These are raw-ish snapshots, not PTRS-specific interpretations.
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      defaultValue: () => getNanoid(10),
    },

    customerId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    xeroTenantId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    xeroContactId: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    contactName: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    contactStatus: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    isSupplier: {
      type: DataTypes.BOOLEAN,
      allowNull: true,
    },

    abn: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    rawPayload: {
      type: DataTypes.JSONB,
      allowNull: false,
    },

    fetchedAt: {
      type: DataTypes.DATE,
      allowNull: false,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsXeroContact = sequelize.define("PtrsXeroContact", attributes, {
    tableName: "tbl_ptrs_xero_contact",
    timestamps: true,
    paranoid: true,
    indexes: [
      {
        fields: ["customerId", "xeroTenantId"],
        name: "ix_ptrs_xero_contact_customer_tenant",
      },
      {
        fields: ["customerId", "xeroTenantId", "xeroContactId"],
        unique: true,
        name: "ux_ptrs_xero_contact_customer_tenant_contact",
      },
    ],
  });

  return PtrsXeroContact;
}
