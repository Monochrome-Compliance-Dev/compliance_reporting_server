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
    budgetLineId: { type: DataTypes.STRING(10), allowNull: false },
    resourceId: { type: DataTypes.STRING(10), allowNull: false },
    assignmentPct: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    role: { type: DataTypes.STRING, allowNull: true },
    rateOverride: { type: DataTypes.DECIMAL(10, 2), allowNull: true },
    startDate: { type: DataTypes.DATEONLY, allowNull: true },
    endDate: { type: DataTypes.DATEONLY, allowNull: true },
    dueDate: { type: DataTypes.DATEONLY, allowNull: true },
    completedAt: { type: DataTypes.DATE, allowNull: true },
    assignedHoursPerWeek: {
      type: DataTypes.DECIMAL(6, 2),
      allowNull: false,
      defaultValue: 0,
    },
    notes: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Assignment = sequelize.define("Assignment", attributes, {
    tableName: "tbl_pulse_assignments",
    timestamps: true,
    paranoid: true, // enable soft-deletes via deletedAt
    indexes: [
      { fields: ["customerId"] },
      { fields: ["budgetLineId"] },
      { fields: ["resourceId"] },
      { fields: ["startDate", "endDate"] },
      { fields: ["dueDate"] },
      { fields: ["completedAt"] },
    ],
  });

  return Assignment;
}
