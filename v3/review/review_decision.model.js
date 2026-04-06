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
    reviewCaseId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    decisionType: {
      type: DataTypes.ENUM(
        "CLASSIFICATION",
        "MAPPING",
        "MATCHING",
        "EXCLUSION",
        "REPROCESSING",
      ),
      allowNull: false,
    },
    decisionOutcome: {
      type: DataTypes.STRING(100),
      allowNull: false,
    },
    applyScope: {
      type: DataTypes.ENUM(
        "SINGLE_RECORD",
        "CASE_RECORDS",
        "CURRENT_SUPPLIER",
        "CURRENT_DATASET",
        "CURRENT_ASSESSMENT",
        "FUTURE_PATTERN",
      ),
      allowNull: false,
      defaultValue: "SINGLE_RECORD",
    },
    reasonCode: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    reviewerNote: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    decisionPayload: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    patternKey: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    reviewedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    reviewedAt: {
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
    tableName: "tbl_v3_review_decision",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        fields: ["customerId", "profileId", "assessmentId"],
      },
      {
        fields: ["reviewCaseId"],
      },
      {
        fields: ["reviewedBy"],
      },
      {
        fields: ["decisionType"],
      },
      {
        fields: ["applyScope"],
      },
      {
        fields: ["patternKey"],
      },
    ],
  };

  const ReviewDecision = sequelize.define(
    "ReviewDecision",
    attributes,
    options,
  );

  return ReviewDecision;
}
