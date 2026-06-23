const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => getNanoid(10),
      primaryKey: true,
    },
    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    label: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },
    description: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    status: {
      type: DataTypes.STRING(50),
      allowNull: false,
      defaultValue: "draft",
    },
    currentStep: {
      type: DataTypes.STRING(50),
      allowNull: true,
      defaultValue: "upload",
    },
    detectedCoverage: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: {},
    },
    metricsSnapshot: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: {},
    },
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const DataHubRun = sequelize.define("DataHubRun", attributes, {
    tableName: "tbl_data_hub_run",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["customerId", "profileId"] },
      { fields: ["status"] },
      { fields: ["currentStep"] },
    ],
  });

  return DataHubRun;
}
