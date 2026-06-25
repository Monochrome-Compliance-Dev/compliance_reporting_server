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

    datasetId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    datasetType: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },

    fieldMapping: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: {},
    },

    mappingStatus: {
      type: DataTypes.STRING(30),
      allowNull: false,
      defaultValue: "draft",
    },

    mappedCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },

    recommendedCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
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

  const DataHubDatasetMap = sequelize.define("DataHubDatasetMap", attributes, {
    tableName: "tbl_data_hub_dataset_map",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["datasetId"] },
      {
        name: "ix_dh_dataset_map_scope",
        fields: ["customerId", "profileId", "datasetId"],
      },
      {
        name: "ux_dh_dataset_map_dataset",
        unique: true,
        fields: ["customerId", "profileId", "datasetId"],
      },
      {
        name: "ix_dh_dataset_map_type",
        fields: ["customerId", "profileId", "datasetType"],
      },
      { fields: ["mappingStatus"] },
    ],
  });

  return DataHubDatasetMap;
}
