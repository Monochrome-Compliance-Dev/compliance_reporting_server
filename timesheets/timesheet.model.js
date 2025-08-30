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
    resourceId: { type: DataTypes.STRING(10), allowNull: false },
    weekKey: { type: DataTypes.DATEONLY, allowNull: false }, // Monday of week
    status: {
      type: DataTypes.STRING(20),
      allowNull: false,
      defaultValue: "draft",
    },
    submittedAt: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.STRING(10), allowNull: true },
    approvedAt: { type: DataTypes.DATE, allowNull: true },
    approvedBy: { type: DataTypes.STRING(10), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Timesheet = sequelize.define("timesheet", attributes, {
    tableName: "tbl_pulse_timesheet",
    timestamps: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["resourceId"] },
      { fields: ["weekKey"] },
      { fields: ["status"] },
    ],
  });

  return Timesheet;
}
