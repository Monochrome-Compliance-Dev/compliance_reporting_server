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
    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    inputSignature: {
      type: DataTypes.STRING(64),
      allowNull: false,
    },
    calculationVersion: {
      type: DataTypes.STRING(64),
      allowNull: false,
    },
    status: {
      type: DataTypes.STRING(20),
      allowNull: false,
      validate: { isIn: [["calculating", "succeeded", "failed"]] },
    },
    aggregateResult: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    provenance: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    errorMessage: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    startedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    completedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  return sequelize.define("PtrsMetricsResult", attributes, {
    tableName: "tbl_ptrs_metrics_result",
    timestamps: true,
    paranoid: false,
    indexes: [
      {
        name: "ptrs_metrics_result_signature_ux",
        unique: true,
        fields: ["customerId", "ptrsId", "inputSignature"],
      },
      {
        name: "ptrs_metrics_result_scope_created_idx",
        fields: ["customerId", "ptrsId", "createdAt"],
      },
    ],
  });
}
