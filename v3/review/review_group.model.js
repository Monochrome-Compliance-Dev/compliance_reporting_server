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
    groupType: {
      type: DataTypes.ENUM(
        "SUPPLIER",
        "DOCUMENT_TYPE",
        "MATCH_CANDIDATE",
        "MAPPING_GAP",
        "EXCLUSION_CANDIDATE",
        "PAYMENT_TIME_ANOMALY",
        "CUSTOM",
      ),
      allowNull: false,
    },
    groupByField: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    groupKey: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM("PENDING", "IN_REVIEW", "RESOLVED", "SKIPPED"),
      allowNull: false,
      defaultValue: "PENDING",
    },
    priority: {
      type: DataTypes.ENUM("LOW", "MEDIUM", "HIGH"),
      allowNull: false,
      defaultValue: "MEDIUM",
    },
    recordCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    reviewedCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    decisionCount: {
      type: DataTypes.INTEGER,
      allowNull: false,
      defaultValue: 0,
    },
    groupSnapshot: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    metadata: {
      type: DataTypes.JSONB,
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
    tableName: "tbl_v3_review_group",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        fields: ["customerId", "profileId", "assessmentId"],
      },
      {
        fields: ["status"],
      },
      {
        fields: ["groupType"],
      },
      {
        fields: ["groupByField"],
      },
      {
        fields: ["groupKey"],
      },
      {
        fields: ["reviewedBy"],
      },
    ],
  };

  const ReviewGroup = sequelize.define("ReviewGroup", attributes, options);

  return ReviewGroup;
}
