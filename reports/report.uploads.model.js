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
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    clientId: { type: DataTypes.STRING(10), allowNull: false },
    reportId: { type: DataTypes.STRING(10), allowNull: false },
    filename: { type: DataTypes.STRING, allowNull: false },
    filepath: { type: DataTypes.STRING, allowNull: false },
    recordCount: { type: DataTypes.INTEGER, allowNull: false },
    uploadedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
  };

  const ReportUploads = sequelize.define("reportUpload", attributes, {
    tableName: "tbl_report_upload",
    timestamps: false,
  });

  ReportUploads.associate = (models) => {
    ReportUploads.belongsTo(models.report, {
      foreignKey: "reportId",
      as: "report",
    });
    ReportUploads.belongsTo(models.client, {
      foreignKey: "clientId",
      as: "client",
    });
  };

  return ReportUploads;
}
