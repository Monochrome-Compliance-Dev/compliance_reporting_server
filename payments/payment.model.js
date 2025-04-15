const { DataTypes } = require("sequelize");
const { update } = require("./payment.service");

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
    createdBy: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    updatedBy: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },
  };

  return sequelize.define("payment", attributes, { tableName: "tbl_payments" });
}
