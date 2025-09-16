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
    customerId: { type: DataTypes.STRING(12), allowNull: false },
    name: { type: DataTypes.STRING(255), allowNull: false },
    abn: { type: DataTypes.STRING(14), allowNull: true },
    accountCode: { type: DataTypes.STRING(64), allowNull: true },
    notes: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const EmployeeRef = sequelize.define("EmployeeRef", attributes, {
    tableName: "tbl_employee_ref",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId", "abn"], unique: false },
      { fields: ["customerId", "accountCode"], unique: false },
    ],
  });

  return EmployeeRef;
}
