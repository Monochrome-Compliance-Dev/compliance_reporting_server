const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      defaultValue: () => getNanoid(10),
    },

    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    profileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    datasetType: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    sourceType: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    sourceName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    originalFileName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    storedFileName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    storagePath: {
      type: DataTypes.STRING(500),
      allowNull: false,
    },

    mimeType: {
      type: DataTypes.STRING(100),
      allowNull: false,
    },

    fileSize: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },

    headers: {
      type: DataTypes.JSONB,
      allowNull: false,
    },

    headersCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },

    rowsCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },

    status: {
      type: DataTypes.STRING(30),
      allowNull: false,
      defaultValue: "available",
    },

    detectedCoverage: {
      type: DataTypes.JSONB,
      allowNull: true,
      defaultValue: {},
    },

    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
      defaultValue: {},
    },

    uploadedBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PlatformDataDataset = sequelize.define(
    "PlatformDataDataset",
    attributes,
    {
      tableName: "tbl_data_hub_dataset",
      timestamps: true,
      paranoid: true,
      indexes: [
        { fields: ["customerId"] },
        { fields: ["profileId"] },
        { fields: ["datasetType"] },
        { fields: ["sourceType"] },
        { fields: ["status"] },
        { fields: ["customerId", "profileId"] },
        { fields: ["customerId", "profileId", "datasetType"] },
      ],
    },
  );

  return PlatformDataDataset;
}
