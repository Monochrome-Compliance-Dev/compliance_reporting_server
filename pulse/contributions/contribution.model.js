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
    resourceId: { type: DataTypes.STRING(10), allowNull: false },
    budgetLineId: { type: DataTypes.STRING(10), allowNull: false },
    assignmentId: { type: DataTypes.STRING(10), allowNull: true },
    effortHours: { type: DataTypes.DECIMAL(5, 2), allowNull: false }, // 0.25 increments
    notes: { type: DataTypes.STRING(500), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Contribution = sequelize.define("Contribution", attributes, {
    tableName: "tbl_pulse_contribution",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["budgetLineId"] },
      { fields: ["resourceId", "budgetLineId"] },
    ],
  });

  return Contribution;
}
