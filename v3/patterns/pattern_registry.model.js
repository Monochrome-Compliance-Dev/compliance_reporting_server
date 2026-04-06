const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      allowNull: false,
      defaultValue: () => getNanoid(10),
    },
    customerId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    sourceSystem: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    datasetRole: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    patternType: {
      type: DataTypes.ENUM(
        "MAPPING",
        "MATCHING",
        "EXCLUSION",
        "CLASSIFICATION",
        "REPROCESSING",
      ),
      allowNull: false,
    },
    patternKey: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    patternDescription: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    actionPayload: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    status: {
      type: DataTypes.ENUM("CANDIDATE", "APPROVED", "REJECTED", "RETIRED"),
      allowNull: false,
      defaultValue: "CANDIDATE",
    },
    approvalScope: {
      type: DataTypes.ENUM(
        "CURRENT_CASE",
        "CURRENT_DATASET",
        "CURRENT_SUPPLIER",
        "CURRENT_PROFILE",
        "CURRENT_CUSTOMER",
        "GLOBAL",
      ),
      allowNull: true,
    },
    approvedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    approvedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    rejectedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    rejectedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    retiredBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    retiredAt: {
      type: DataTypes.DATE,
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
    tableName: "tbl_v3_pattern_registry",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        fields: ["customerId", "profileId"],
      },
      {
        fields: ["patternType"],
      },
      {
        fields: ["patternKey"],
      },
      {
        fields: ["status"],
      },
      {
        fields: ["sourceSystem", "datasetRole"],
      },
    ],
  };

  const PatternRegistry = sequelize.define(
    "PatternRegistry",
    attributes,
    options,
  );

  return PatternRegistry;
}
