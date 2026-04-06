const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    patternId: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      allowNull: false,
    },
    timesSeen: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    timesReviewed: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    timesConfirmed: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    timesOverridden: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    timesRejected: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    lastSeenAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    lastReviewedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    lastConfirmedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    lastOverriddenAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    lastRejectedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    autoApplyEligible: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: false,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    deletedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const options = {
    tableName: "tbl_v3_pattern_stats",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        fields: ["autoApplyEligible"],
      },
      {
        fields: ["lastSeenAt"],
      },
      {
        fields: ["lastReviewedAt"],
      },
    ],
  };

  const PatternStats = sequelize.define("PatternStats", attributes, options);

  return PatternStats;
}
