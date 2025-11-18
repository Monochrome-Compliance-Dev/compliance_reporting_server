const { DataTypes, Op } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    customerId: { type: DataTypes.STRING, allowNull: false },
    feature: { type: DataTypes.STRING, allowNull: false },
    status: {
      type: DataTypes.STRING,
      allowNull: false,
      defaultValue: "active",
    },
    source: {
      type: DataTypes.STRING,
      allowNull: false,
      defaultValue: "stripe",
    },
    validFrom: { type: DataTypes.DATE, allowNull: false },
    validTo: { type: DataTypes.DATE, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const FeatureEntitlement = sequelize.define(
    "FeatureEntitlement",
    attributes,
    {
      tableName: "tbl_feature_entitlements",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          unique: true,
          fields: ["customerId", "feature"],
        },
      ],
    }
  );

  return FeatureEntitlement;
}
