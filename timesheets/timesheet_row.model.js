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
    timesheetId: { type: DataTypes.STRING(10), allowNull: false },
    date: { type: DataTypes.DATEONLY, allowNull: false },
    engagementId: { type: DataTypes.STRING(10), allowNull: true },
    budgetItemId: { type: DataTypes.STRING(10), allowNull: true },
    hours: { type: DataTypes.DECIMAL(5, 2), allowNull: false, defaultValue: 0 },
    billable: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true },
    rate: { type: DataTypes.DECIMAL(10, 2), allowNull: true },
    notes: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const TimesheetRow = sequelize.define("timesheet_row", attributes, {
    tableName: "tbl_pulse_timesheet_row",
    timestamps: true,
    paranoid: true, // enable soft-deletes via deletedAt
    indexes: [
      { fields: ["customerId"] },
      { fields: ["timesheetId"] },
      { fields: ["engagementId"] },
      { fields: ["date"] },
      { fields: ["billable"] },
    ],
  });

  return TimesheetRow;
}
