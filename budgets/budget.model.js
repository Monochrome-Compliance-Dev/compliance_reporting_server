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
    customerId: { type: DataTypes.STRING, allowNull: false },
    engagementId: {
      type: DataTypes.STRING(10),
      allowNull: true,
      defaultValue: null,
    },
    name: { type: DataTypes.STRING, allowNull: false },
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

  const Budget = sequelize.define("budget", attributes, {
    tableName: "tbl_pulse_budget",
    timestamps: true,
    paranoid: true, // enables soft delete via deletedAt
  });

  return Budget;
}
