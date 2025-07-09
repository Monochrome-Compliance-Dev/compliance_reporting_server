const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const ESGIndicator = sequelize.define(
    "ESGIndicator",
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
      reportingPeriodId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      code: { type: DataTypes.STRING, allowNull: false, unique: true },
      name: { type: DataTypes.STRING, allowNull: false },
      description: DataTypes.TEXT,
      category: {
        type: DataTypes.ENUM("environment", "social", "governance"),
        allowNull: false,
      },
    },
    { tableName: "tbl_esg_indicators", timestamps: true, paranoid: true }
  );
  return ESGIndicator;
};
