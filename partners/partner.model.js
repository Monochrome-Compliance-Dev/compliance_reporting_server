const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const Partner = sequelize.define(
    "Partner",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        allowNull: false,
        defaultValue: () => nanoid(10),
      },
      name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      contactName: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      contactEmail: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      discountRate: {
        type: DataTypes.DECIMAL(5, 2),
        allowNull: true,
        comment:
          "Percentage discount applied to partner's billing total (e.g., 15.00)",
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
      createdBy: { type: DataTypes.STRING(10), allowNull: false },
      updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    },
    {
      tableName: "tbl_partner",
      timestamps: true,
      paranoid: true,
    }
  );

  Partner.associate = (models) => {
    Partner.hasMany(models.Client, { foreignKey: "partnerId" });
  };

  return Partner;
};
