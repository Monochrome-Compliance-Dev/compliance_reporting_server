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
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    trackableId: { type: DataTypes.STRING(10), allowNull: true },
    isActive: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true },
    startsAt: { type: DataTypes.DATEONLY, allowNull: true },
    endsAt: { type: DataTypes.DATEONLY, allowNull: true },
    reason: { type: DataTypes.STRING(200), allowNull: true },
    name: { type: DataTypes.STRING(200), allowNull: false },
    status: {
      type: DataTypes.STRING(10),
      allowNull: false,
      defaultValue: "draft",
    }, // draft | final | archived
    version: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 1 },
    currency: {
      type: DataTypes.STRING(3),
      allowNull: false,
      defaultValue: "AUD",
    },
    notes: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Budget = sequelize.define("Budget", attributes, {
    tableName: "tbl_pulse_budget",
    timestamps: true,
    paranoid: true, // enables soft delete via deletedAt
    indexes: [
      { fields: ["customerId", "trackableId", "isActive"] },
      { fields: ["customerId", "trackableId", "version"] },
    ],
  });

  return Budget;
}
