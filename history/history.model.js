const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.INTEGER,
      autoIncrement: true,
      primaryKey: true,
    },
    reportName: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    createdBy: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    updatedBy: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    submittedDate: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    submittedBy: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    regulatorStatus: {
      type: DataTypes.ENUM("Received", "Accepted", "Rejected"),
      allowNull: false,
      defaultValue: "Received",
    },
    statusUpdatedDate: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    tenantId: {
      type: DataTypes.STRING,
      allowNull: false,
    },
  };

  return sequelize.define("history", attributes, {
    tableName: "tbl_history",
  });
}
