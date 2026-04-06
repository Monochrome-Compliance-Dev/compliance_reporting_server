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
      allowNull: false,
    },
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    assessmentId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    datasetId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    caseType: {
      type: DataTypes.ENUM(
        "MAPPING_REVIEW",
        "VALIDATION_REVIEW",
        "MATCH_REVIEW",
        "CLASSIFICATION_REVIEW",
        "EXCLUSION_REVIEW",
      ),
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM(
        "OPEN",
        "ASSIGNED",
        "IN_REVIEW",
        "RESOLVED",
        "DISMISSED",
      ),
      allowNull: false,
      defaultValue: "OPEN",
    },
    priority: {
      type: DataTypes.ENUM("LOW", "MEDIUM", "HIGH"),
      allowNull: false,
      defaultValue: "MEDIUM",
    },
    patternKey: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    patternDescription: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    sourceSystem: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    datasetRole: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    recordCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    totalValue: {
      type: DataTypes.DECIMAL(18, 2),
      allowNull: true,
    },
    smallBusinessCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    smallBusinessValue: {
      type: DataTypes.DECIMAL(18, 2),
      allowNull: true,
    },
    oldestRecordDate: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    newestRecordDate: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    queueReasonCodes: {
      type: DataTypes.JSONB,
      allowNull: true,
      defaultValue: [],
    },
    evidencePayload: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    assignedTo: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    assignedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
    resolvedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    resolvedAt: {
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
    tableName: "tbl_v3_review_case",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        fields: ["customerId", "profileId", "assessmentId"],
      },
      {
        fields: ["status", "priority"],
      },
      {
        fields: ["caseType"],
      },
      {
        fields: ["patternKey"],
      },
      {
        fields: ["datasetId"],
      },
    ],
  };

  const ReviewCase = sequelize.define("ReviewCase", attributes, options);

  return ReviewCase;
}
