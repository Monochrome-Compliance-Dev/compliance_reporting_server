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
    runId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    role: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },
    sourceType: {
      type: DataTypes.STRING(50),
      allowNull: false,
      defaultValue: "csv",
    },
    sourceName: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },
    originalFileName: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },
    storedFileName: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },
    storagePath: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    mimeType: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    fileSize: {
      type: DataTypes.BIGINT,
      allowNull: true,
    },
    headers: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: [],
    },
    headersCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    rowsCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    status: {
      type: DataTypes.STRING(50),
      allowNull: false,
      defaultValue: "uploaded",
    },
    detectedCoverage: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: {},
    },
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    uploadedBy: {
      type: DataTypes.STRING(10),
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

  const DataHubDataset = sequelize.define("DataHubDataset", attributes, {
    tableName: "tbl_data_hub_dataset",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["runId"] },
      { fields: ["customerId"] },
      { fields: ["customerId", "profileId"] },
      { fields: ["runId", "role"] },
      { fields: ["status"] },
    ],
  });

  return DataHubDataset;
}
