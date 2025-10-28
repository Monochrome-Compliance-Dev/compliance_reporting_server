const { DataTypes } = require("sequelize");

// Explicit mapping of a customer to a PTRS profile (no implicit fallbacks)
module.exports = (sequelize) => {
  const ptrs_customer_profile = sequelize.define(
    "ptrs_customer_profile",
    {
      id: {
        type: DataTypes.STRING,
        primaryKey: true,
        defaultValue: async () => {
          const nanoid = await getNanoid();
          return nanoid();
        },
      },
      // Customer tenant identifier (e.g. auth/tenant id). Unique mapping per customer.
      customerId: {
        type: DataTypes.STRING(64),
        primaryKey: true,
        allowNull: false,
      },
      // The profile id to use for this customer (FK → ptrs_profile.id)
      profileId: {
        type: DataTypes.STRING(64),
        allowNull: false,
      },

      // Audit
      createdBy: { type: DataTypes.STRING(64), allowNull: true },
      updatedBy: { type: DataTypes.STRING(64), allowNull: true },
    },
    {
      tableName: "tbl_ptrs_customer_profile",
      timestamps: true,
      underscored: false,
      indexes: [
        { unique: true, fields: ["customerId"] },
        { fields: ["profileId"] },
      ],
    }
  );

  ptrs_customer_profile.associate = (models) => {
    if (models.ptrs_profile) {
      ptrs_customer_profile.belongsTo(models.ptrs_profile, {
        as: "profile",
        foreignKey: "profileId",
        targetKey: "id",
        onDelete: "RESTRICT",
        onUpdate: "CASCADE",
      });
    }
  };

  return ptrs_customer_profile;
};
