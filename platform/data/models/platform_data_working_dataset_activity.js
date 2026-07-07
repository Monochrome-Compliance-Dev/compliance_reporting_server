const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = (sequelize) => {
  const PlatformDataWorkingDatasetActivity = sequelize.define(
    "PlatformDataWorkingDatasetActivity",
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
      workingDatasetId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      activityType: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      stepNumber: {
        type: DataTypes.INTEGER,
        allowNull: true,
      },
      summary: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      details: {
        type: DataTypes.JSONB,
        allowNull: false,
        defaultValue: {},
      },
      relatedCapability: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      relatedRecordId: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      createdBy: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
    },
    {
      tableName: "tbl_data_working_dataset_activity",
      timestamps: true,
      updatedAt: false,
      indexes: [
        {
          name: "idx_data_wda_scope_dataset",
          fields: ["customerId", "profileId", "workingDatasetId"],
        },
        {
          name: "idx_data_wda_activity_type",
          fields: ["activityType"],
        },
        {
          name: "idx_data_wda_created_at",
          fields: ["createdAt"],
        },
      ],
    },
  );

  return PlatformDataWorkingDatasetActivity;
};
