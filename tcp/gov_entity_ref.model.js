const { DataTypes } = require("sequelize");
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
    // Australian Business Number (11 digits) stored as string; optional if only a name is available
    abn: { type: DataTypes.STRING(14), allowNull: true, unique: true },
    name: { type: DataTypes.STRING(255), allowNull: false },
    // e.g. 'ATO', 'StateRevenue', 'LocalCouncil', 'Utility', 'University', etc.
    category: { type: DataTypes.STRING(64), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const GovEntityRef = sequelize.define("GovEntityRef", attributes, {
    tableName: "tbl_gov_entity_ref",
    timestamps: true,
    paranoid: true, // soft-deletes via deletedAt
  });

  return GovEntityRef;
}
