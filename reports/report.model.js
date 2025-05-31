const { DataTypes, INTEGER } = require("sequelize");
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
    currentStep: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
    reportStatus: {
      type: DataTypes.ENUM(
        "Created",
        "Cancelled",
        "In Progress",
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
    // Foreign key for Client
    clientId: { type: DataTypes.STRING(10), allowNull: false },
  };

  const Report = sequelize.define("report", attributes, {
    tableName: "tbl_report",
    timestamps: true,
  });

  Report.associate = (models) => {
    Report.belongsTo(models.Client, { foreignKey: "clientId" });
    models.Client.hasMany(Report, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });

    Report.hasMany(models.Tcp, { foreignKey: "reportId", onDelete: "CASCADE" });
    models.Tcp.belongsTo(Report, {
      foreignKey: "reportId",
      onDelete: "CASCADE",
    });
  };

  return Report;
}
