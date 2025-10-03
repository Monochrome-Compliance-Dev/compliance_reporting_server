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

    // New parent keys
    budgetId: { type: DataTypes.STRING(10), allowNull: false },
    sectionId: { type: DataTypes.STRING(10), allowNull: true },

    // New label for resource level/role (e.g., "Auditor (2nd year)")
    resourceLabel: { type: DataTypes.STRING(200), allowNull: true },

    // Deprecated: kept for backward compatibility; prefer resourceLabel going forward
    sectionName: { type: DataTypes.STRING, allowNull: true },
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

    // Stable sort within a budget/section
    order: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },

    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const BudgetItem = sequelize.define("BudgetItem", attributes, {
    tableName: "tbl_pulse_budget_item",
    timestamps: true,
    paranoid: true, // enables soft delete via deletedAt
    indexes: [
      { fields: ["customerId"] },
      {
        unique: true,
        fields: ["customerId", "budgetId", "sectionId", "order"],
      },
    ],
    // Keep a simple lookup index for common queries
    // (Sequelize allows multiple index definitions; leaving as separate entries)
  });

  return BudgetItem;
}
