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
    budgetId: { type: DataTypes.STRING(10), allowNull: false },
    name: { type: DataTypes.STRING, allowNull: false },
    notes: { type: DataTypes.TEXT, allowNull: true },
    order: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const BudgetSection = sequelize.define("BudgetSection", attributes, {
    tableName: "tbl_pulse_budget_section",
    timestamps: true,
    paranoid: true, // enables soft delete via deletedAt
    indexes: [
      { fields: ["customerId"] },
      { unique: true, fields: ["customerId", "budgetId", "order"] },
    ],
  });

  return BudgetSection;
}
