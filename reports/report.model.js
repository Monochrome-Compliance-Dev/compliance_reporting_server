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
    ReportingPeriodStartDate: { type: DataTypes.DATE, allowNull: false },
    ReportingPeriodEndDate: { type: DataTypes.DATE, allowNull: false },
    code: { type: DataTypes.STRING, allowNull: false },
    reportName: { type: DataTypes.STRING, allowNull: false },
    submittedDate: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.STRING(10), allowNull: true },
    reportStatus: {
      type: DataTypes.ENUM(
        "Created",
        "Cancelled",
        "Updated",
        "Received",
        "Accepted",
        "Rejected",
        "Submitted"
      ),
      allowNull: false,
      defaultValue: "Created",
    },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  return sequelize.define("report", attributes, {
    tableName: "tbl_report",
    timestamps: true,
    charset: "utf8mb4",
    collate: "utf8mb4_0900_ai_ci",
  });
}
