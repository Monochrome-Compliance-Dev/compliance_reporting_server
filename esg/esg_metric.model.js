const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const ESGMetric = sequelize.define(
    "ESGMetric",
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
      indicatorId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      reportingPeriodId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      value: {
        type: DataTypes.DECIMAL,
        allowNull: false,
      },
      unit: {
        type: DataTypes.STRING,
        allowNull: true,
      },
    },
    {
      tableName: "tbl_esg_metrics",
      timestamps: true,
    }
  );
  return ESGMetric;
};
