const { DataTypes } = require("sequelize");

// PTRS profile holds the customer-specific blueprint (synonyms, joins, row rules, etc.)
// We store the JSON payload in JSONB columns for auditability & versioning.
module.exports = (sequelize) => {
  const ptrs_profile = sequelize.define(
    "ptrs_profile",
    {
      // e.g. "veolia" — stable, human-readable key
      id: {
        type: DataTypes.STRING(64),
        primaryKey: true,
        allowNull: false,
      },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      // Display name for UI
      label: {
        type: DataTypes.STRING(255),
        allowNull: false,
      },
      // Optional inheritance from a base profile/blueprint id
      extends: {
        type: DataTypes.STRING(64),
        allowNull: true,
      },
      // JSONB payloads (nullable)
      synonyms: { type: DataTypes.JSONB, allowNull: true },
      fallbacks: { type: DataTypes.JSONB, allowNull: true },
      defaults: { type: DataTypes.JSONB, allowNull: true },
      joins: { type: DataTypes.JSONB, allowNull: true },
      rowRules: { type: DataTypes.JSONB, allowNull: true },

      // Versioning & lifecycle flags
      version: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 1 },
      isActive: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
        defaultValue: true,
      },

      // Audit
      createdBy: { type: DataTypes.STRING(64), allowNull: true },
      updatedBy: { type: DataTypes.STRING(64), allowNull: true },
    },
    {
      tableName: "tbl_ptrs_profile",
      timestamps: true,
      underscored: false,
      indexes: [{ fields: ["isActive"] }, { fields: ["extends"] }],
    }
  );

  ptrs_profile.associate = (models) => {
    // One profile can be mapped to many customers
    if (models.ptrs_customer_profile) {
      ptrs_profile.hasMany(models.ptrs_customer_profile, {
        as: "customerMappings",
        foreignKey: "profileId",
        sourceKey: "id",
        onDelete: "RESTRICT",
        onUpdate: "CASCADE",
      });
    }
  };

  return ptrs_profile;
};
