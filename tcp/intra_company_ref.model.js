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
    counterpartyAbn: { type: DataTypes.STRING(14), allowNull: true },
    counterpartyName: { type: DataTypes.STRING(255), allowNull: false },
    accountCodePattern: { type: DataTypes.STRING(64), allowNull: true }, // e.g., 'INT%'
    notes: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const IntraCompanyRef = sequelize.define("IntraCompanyRef", attributes, {
    tableName: "tbl_intra_company_ref",
    timestamps: true,
    paranoid: true,
    indexes: [{ fields: ["customerId", "counterpartyAbn"], unique: false }],
  });

  return IntraCompanyRef;
}
