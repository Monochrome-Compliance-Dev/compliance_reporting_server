const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = (sequelize) => {
  const PlatformDataWorkingDataset = sequelize.define(
    "PlatformDataWorkingDataset",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        allowNull: false,
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
      sourceDatasetId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      workingName: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      datasetType: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      status: {
        type: DataTypes.STRING,
        allowNull: false,
        defaultValue: "in_progress",
      },
      currentStepNumber: {
        type: DataTypes.INTEGER,
        allowNull: true,
      },
      storagePath: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      storedFileName: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      mimeType: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      fileSize: {
        type: DataTypes.BIGINT,
        allowNull: false,
      },
      headers: {
        type: DataTypes.JSONB,
        allowNull: false,
        defaultValue: [],
      },
      headersCount: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      rowsCount: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      lineage: {
        type: DataTypes.JSONB,
        allowNull: false,
        defaultValue: {},
      },
      meta: {
        type: DataTypes.JSONB,
        allowNull: false,
        defaultValue: {},
      },
      activeEditorUserId: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
      activeEditorSessionId: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      activeEditorStartedAt: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      activeEditorLastSeenAt: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      activeEditorExpiresAt: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      finalisedAt: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      finalisedBy: {
        type: DataTypes.STRING(10),
        allowNull: true,
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
    },
    {
      tableName: "tbl_data_working_dataset",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          fields: ["customerId", "profileId"],
        },
        {
          fields: ["sourceDatasetId"],
        },
        {
          fields: ["customerId", "profileId", "status"],
        },
        {
          fields: ["activeEditorUserId"],
        },
      ],
    },
  );

  return PlatformDataWorkingDataset;
};
