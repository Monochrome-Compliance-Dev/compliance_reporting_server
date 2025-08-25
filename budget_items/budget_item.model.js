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
    engagementId: { type: DataTypes.STRING(10), allowNull: false },
    activity: { type: DataTypes.STRING, allowNull: false },
    billingType: {
      type: DataTypes.STRING(10),
      allowNull: false,
      defaultValue: "hourly",
    }, // 'hourly' | 'fixed'
    hours: { type: DataTypes.DECIMAL(6, 2), allowNull: false, defaultValue: 0 },
    rate: { type: DataTypes.DECIMAL(10, 2), allowNull: false, defaultValue: 0 },
    amount: {
      type: DataTypes.DECIMAL(12, 2),
      allowNull: false,
      defaultValue: 0,
    },
    billable: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true },
    notes: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const BudgetItem = sequelize.define("budget_item", attributes, {
    tableName: "tbl_budget_item",
    timestamps: true,
  });

  return BudgetItem;
}
