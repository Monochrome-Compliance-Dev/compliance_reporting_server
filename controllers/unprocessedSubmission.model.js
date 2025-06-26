const { DataTypes } = require("sequelize");
const { sequelize } = require("../db/database");

const UnprocessedSubmission = sequelize.define(
  "UnprocessedSubmission",
  {
    id: {
      type: DataTypes.INTEGER,
      autoIncrement: true,
      primaryKey: true,
    },
    email: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    contactName: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    contactPhone: {
      type: DataTypes.STRING,
    },
    businessName: {
      type: DataTypes.STRING,
    },
    abn: {
      type: DataTypes.STRING,
    },
    fileType: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    filePath: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    stripePaymentId: {
      type: DataTypes.STRING,
    },
    status: {
      type: DataTypes.STRING,
      defaultValue: "pending",
    },
  },
  {
    tableName: "UnprocessedSubmissions",
    timestamps: true,
  }
);

module.exports = UnprocessedSubmission;
