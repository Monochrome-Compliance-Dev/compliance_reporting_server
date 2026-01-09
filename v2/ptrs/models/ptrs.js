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
    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    label: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },
    periodStart: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    periodEnd: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    reportingEntityName: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },
    status: {
      type: DataTypes.STRING(50),
      allowNull: false,
      defaultValue: "draft", // draft|uploading|mapped|staged|rules_applied|...
    },
    currentStep: {
      type: DataTypes.STRING(50),
      allowNull: true, // create|tables|map|stage|rules|validate|sbi|metrics|report
    },
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    reportPreviewDraft: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: {},
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

  const Ptrs = sequelize.define("Ptrs", attributes, {
    tableName: "tbl_ptrs",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["customerId", "profileId"] },
      { fields: ["status"] },
    ],
  });

  return Ptrs;
}
