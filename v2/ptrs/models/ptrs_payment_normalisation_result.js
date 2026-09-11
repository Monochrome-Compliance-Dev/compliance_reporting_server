const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  return sequelize.define(
    "PtrsPaymentNormalisationResult",
    {
      id: {
        type: DataTypes.STRING(10),
        defaultValue: () => getNanoid(10),
        primaryKey: true,
      },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      ptrsId: { type: DataTypes.STRING(10), allowNull: false },
      profileId: { type: DataTypes.STRING(10), allowNull: false },
      stageExecutionRunId: { type: DataTypes.STRING(10), allowNull: false },
      stageInputHash: { type: DataTypes.STRING(64), allowNull: false },
      normalisationInputRevision: { type: DataTypes.BIGINT, allowNull: false },
      inputSignature: { type: DataTypes.STRING(64), allowNull: false },
      calculationVersion: { type: DataTypes.STRING(64), allowNull: false },
      status: {
        type: DataTypes.STRING(20),
        allowNull: false,
        validate: { isIn: [["calculating", "succeeded", "failed"]] },
      },
      summary: { type: DataTypes.JSONB, allowNull: true },
      errorMessage: { type: DataTypes.TEXT, allowNull: true },
      createdBy: { type: DataTypes.STRING(10), allowNull: true },
      startedAt: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      completedAt: { type: DataTypes.DATE, allowNull: true },
    },
    {
      tableName: "tbl_ptrs_payment_normalisation_result",
      timestamps: true,
      paranoid: false,
      indexes: [
        {
          name: "ptrs_payment_normalisation_result_identity_ux",
          unique: true,
          fields: [
            "customerId",
            "ptrsId",
            "inputSignature",
            "calculationVersion",
          ],
        },
        {
          name: "ptrs_payment_normalisation_result_scope_created_idx",
          fields: ["customerId", "ptrsId", "createdAt"],
        },
      ],
    },
  );
}
