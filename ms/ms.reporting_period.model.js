const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const MSReportingPeriod = sequelize.define(
    "MSReportingPeriod",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        defaultValue: () => nanoid(10),
      },
      customerId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      name: { type: DataTypes.STRING, allowNull: false },
      startDate: { type: DataTypes.DATEONLY, allowNull: false },
      endDate: { type: DataTypes.DATEONLY, allowNull: false },
      status: {
        type: DataTypes.ENUM("Draft", "PendingApproval", "Approved"),
        defaultValue: "Draft",
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
      tableName: "tbl_ms_reporting_periods",
      timestamps: true,
      paranoid: true,
    }
  );

  return MSReportingPeriod;
};
