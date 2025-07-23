const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const MSSupplierRisk = sequelize.define(
    "MSSupplierRisk",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        defaultValue: () => nanoid(10),
      },
      clientId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      country: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      risk: {
        type: DataTypes.ENUM("Low", "Medium", "High"),
        allowNull: false,
      },
      reviewed: {
        type: DataTypes.DATE,
      },
      createdBy: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      updatedBy: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
    },
    { tableName: "tbl_ms_supplier_risks", timestamps: true, paranoid: true }
  );
  return MSSupplierRisk;
};
