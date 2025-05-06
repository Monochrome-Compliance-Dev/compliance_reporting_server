const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },

    // TERMS
    // Current reporting period
    mostCommonPaymentTerm: { type: DataTypes.INTEGER, allowNull: false },
    receivableTermComparison: {
      type: DataTypes.ENUM("Shorter", "Same", "Longer"),
      allowNull: false,
    },
    rangeMinCurrent: { type: DataTypes.INTEGER, allowNull: false },
    rangeMaxCurrent: { type: DataTypes.INTEGER, allowNull: false },

    // Next reporting period (estimates)
    expectedMostCommonPaymentTerm: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    expectedRangeMin: { type: DataTypes.INTEGER, allowNull: false },
    expectedRangeMax: { type: DataTypes.INTEGER, allowNull: false },

    // TIMES
    // Summary statistics
    averagePaymentTime: { type: DataTypes.FLOAT, allowNull: false },
    medianPaymentTime: { type: DataTypes.FLOAT, allowNull: false },
    percentile80: { type: DataTypes.INTEGER, allowNull: false },
    percentile95: { type: DataTypes.INTEGER, allowNull: false },

    // Compliance indicators
    paidWithinTermsPercent: { type: DataTypes.FLOAT, allowNull: false },
    paidWithin30DaysPercent: { type: DataTypes.FLOAT, allowNull: false },
    paid31To60DaysPercent: { type: DataTypes.FLOAT, allowNull: false },
    paidOver60DaysPercent: { type: DataTypes.FLOAT, allowNull: false },

    // Metadata
    createdBy: { type: DataTypes.INTEGER, allowNull: true },
    updatedBy: { type: DataTypes.INTEGER, allowNull: true },
  };

  return sequelize.define("tat", attributes, { tableName: "tbl_tat" });
}
