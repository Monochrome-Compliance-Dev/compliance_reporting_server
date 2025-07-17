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
      unitId: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
      isTemplate: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
        defaultValue: false,
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
      tableName: "tbl_esg_metrics",
      timestamps: true,
      paranoid: true,
    }
  );
  return ESGMetric;
};
