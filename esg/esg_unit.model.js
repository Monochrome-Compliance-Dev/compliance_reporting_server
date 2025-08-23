const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const Unit = sequelize.define(
    "Unit",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
      },
      customerId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      symbol: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      description: {
        type: DataTypes.TEXT,
        allowNull: true,
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
    {
      tableName: "tbl_esg_units",
      timestamps: true,
      paranoid: true,
    }
  );
  return Unit;
};
