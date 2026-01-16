const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

/**
 * PTRS v2 Xero Organisation cache
 *
 * Purpose:
 * - Cache organisation metadata (ABN/TaxNumber, Name, PaymentTerms, etc.) so PTRS can map payer details
 * - Provide stable org naming for FE tenant/org selection
 *
 * Notes:
 * - `xeroTenantId` is the tenantId returned from Xero connections.
 * - `xeroOrganisationId` is the OrganisationID returned from Xero accounting APIs.
 * - Uniqueness is scoped to customerId.
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      defaultValue: () => getNanoid(10),
    },

    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // Optional: allow linking to a specific run (useful during dev / MVP); orgs may also be reused across runs.
    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    // Xero connections tenantId
    xeroTenantId: {
      type: DataTypes.UUID,
      allowNull: false,
    },

    // Xero accounting OrganisationID
    xeroOrganisationId: {
      type: DataTypes.UUID,
      allowNull: false,
    },

    name: {
      type: DataTypes.STRING,
      allowNull: false,
    },

    legalName: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    registrationNumber: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    taxNumber: {
      type: DataTypes.STRING,
      allowNull: true,
    },

    paymentTerms: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    // Store the raw organisation payload for flexibility (MVP-friendly)
    rawPayload: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    fetchedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },

    createdBy: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    updatedBy: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsXeroOrganisation = sequelize.define(
    "PtrsXeroOrganisation",
    attributes,
    {
      tableName: "tbl_ptrs_xero_organisation",
      schema: "public",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          name: "ix_ptrs_xero_org_customer_tenant",
          unique: true,
          fields: ["customerId", "xeroTenantId"],
        },
        {
          name: "ix_ptrs_xero_org_customer_orgid",
          unique: true,
          fields: ["customerId", "xeroOrganisationId"],
        },
        {
          name: "ix_ptrs_xero_org_customer_ptrs",
          unique: false,
          fields: ["customerId", "ptrsId"],
        },
      ],
    }
  );

  return PtrsXeroOrganisation;
}
