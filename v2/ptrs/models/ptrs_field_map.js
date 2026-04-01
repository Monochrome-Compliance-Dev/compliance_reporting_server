const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

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

    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // Profile-scoped mappings are required (different report entities can have different mappings).
    // IMPORTANT:
    // - The live foreign key for `profileId` currently points to `tbl_customer_profile(id)`.
    // - `tbl_ptrs_profile` exists but is not currently populated/used by the application.
    // - Do NOT repoint this field back to `tbl_ptrs_profile` unless the application is fully migrated
    //   to create and use real PTRS profile rows.
    // - Longer term, we should decide whether to:
    //     1) keep using `tbl_customer_profile` as the source of truth, or
    //     2) properly adopt `tbl_ptrs_profile` and migrate all callers/data.
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    // Canonical (normalised) field name used by staging/rules/metrics/reporting
    // e.g. "invoice_payment_terms", "payment_date", "payer_entity_abn"
    canonicalField: {
      type: DataTypes.STRING(100),
      allowNull: false,
    },

    // Where the value comes from.
    // Common roles: "main" | "entitystructure" | "termschanges" | "custom" | "computed"
    sourceRole: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    // The source column/header name within the role.
    // For "custom" / "computed" sources, this can be the custom field key.
    sourceColumn: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    // Optional transform identifier (e.g. "asof_terms_change")
    transformType: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    // Optional transform configuration (role-specific)
    transformConfig: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    // Optional free-form metadata for UI/debugging
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },

    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsFieldMap = sequelize.define("PtrsFieldMap", attributes, {
    // NOTE:
    // This model represents `tbl_ptrs_field_map` only.
    // The database-level foreign key for `profileId` is currently expected to reference
    // `tbl_customer_profile(id)` in the live environment.
    // If a future migration reintroduces `tbl_ptrs_profile` as the real parent table,
    // update the database constraint and this model commentary together.
    tableName: "tbl_ptrs_field_map",
    timestamps: true,
    paranoid: true,
    indexes: [
      // Fast scope filtering
      {
        name: "ix_ptrs_field_map_scope",
        fields: ["customerId", "ptrsId", "profileId"],
      },

      // One mapping per canonical field per profile per ptrs
      {
        name: "ux_ptrs_field_map_canon",
        unique: true,
        fields: ["customerId", "ptrsId", "profileId", "canonicalField"],
      },

      // Useful for quickly finding where a source role is used
      {
        name: "ix_ptrs_field_map_source_role",
        fields: ["customerId", "ptrsId", "profileId", "sourceRole"],
      },

      // Optional helper for UI search / reverse lookup (non-unique)
      {
        name: "ix_ptrs_field_map_source_col",
        fields: ["customerId", "ptrsId", "profileId", "sourceColumn"],
      },
    ],
  });

  return PtrsFieldMap;
}
