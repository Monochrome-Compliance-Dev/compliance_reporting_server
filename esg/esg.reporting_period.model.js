const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const ReportingPeriod = sequelize.define(
    "ReportingPeriod",
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
      name: { type: DataTypes.STRING, allowNull: false },
      startDate: { type: DataTypes.DATEONLY, allowNull: false },
      endDate: { type: DataTypes.DATEONLY, allowNull: false },
    },
    {
      tableName: "tbl_esg_reporting_periods",
      timestamps: true,
      paranoid: true,
    }
  );

  return ReportingPeriod;
};
