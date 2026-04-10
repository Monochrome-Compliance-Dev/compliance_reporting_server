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
    reviewGroupId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    datasetId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    stageRowId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    rowNo: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },
    recordStatus: {
      type: DataTypes.ENUM("PENDING", "REVIEWED", "SKIPPED", "RESOLVED"),
      allowNull: false,
      defaultValue: "PENDING",
    },
    suggestedAction: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    appliedAction: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    reasonCode: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },
    recordSnapshot: {
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
    tableName: "tbl_v3_review_group_record",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        fields: ["customerId", "profileId", "assessmentId"],
      },
      {
        fields: ["reviewGroupId"],
      },
      {
        fields: ["datasetId"],
      },
      {
        fields: ["stageRowId"],
      },
      {
        fields: ["recordStatus"],
      },
      {
        fields: ["reviewedBy"],
      },
    ],
  };

  const ReviewGroupRecord = sequelize.define(
    "ReviewGroupRecord",
    attributes,
    options,
  );

  return ReviewGroupRecord;
}
