const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    ReportingPeriodStartDate: { type: DataTypes.DATE, allowNull: false },
    ReportingPeriodEndDate: { type: DataTypes.DATE, allowNull: false },
    reportName: { type: DataTypes.STRING, allowNull: false },
    createdBy: { type: DataTypes.INTEGER, allowNull: false },
    updatedBy: { type: DataTypes.INTEGER, allowNull: true },
    submittedDate: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.INTEGER, allowNull: true },
    reportStatus: {
      type: DataTypes.ENUM(
        "Created",
        "Cancelled",
        "Received",
        "Accepted",
        "Rejected"
      ),
      allowNull: false,
      defaultValue: "Created",
    },
    clientId: { type: DataTypes.INTEGER, allowNull: false },
  };

  return sequelize.define("report", attributes, { tableName: "tbl_reports" });
}
