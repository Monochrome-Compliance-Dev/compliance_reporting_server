const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    StandardPaymentPeriodInCalendarDays: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    ChangesToStandardPaymentPeriod: { type: DataTypes.STRING, allowNull: true },
    DetailsOfChangesToStandardPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    ShortestActualStandardPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    ChangeShortestActualPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    DetailChangeShortestActualPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    LongestActualStandardPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    ChangeLongestActualPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    DetailChangeLongestActualPaymentPeriod: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    created: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    updated: { type: DataTypes.DATE },
  };

  return sequelize.define("payment", attributes, { tableName: "tbl_payments" });
}
