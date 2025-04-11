const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    reportName: { type: DataTypes.STRING, allowNull: false },
    createdBy: { type: DataTypes.STRING, allowNull: false },
    updatedBy: { type: DataTypes.STRING, allowNull: true },
    submittedDate: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.STRING, allowNull: true },
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
    statusUpdatedDate: { type: DataTypes.DATE, allowNull: true },
    clientId: { type: DataTypes.STRING, allowNull: false },
  };

  return sequelize.define("history", attributes, {
    tableName: "tbl_history",
  });
}
