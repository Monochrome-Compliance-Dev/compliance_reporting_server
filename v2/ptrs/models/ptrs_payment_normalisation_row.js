const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  return sequelize.define(
    "PtrsPaymentNormalisationRow",
    {
      id: { type: DataTypes.BIGINT, autoIncrement: true, primaryKey: true },
      normalisationResultId: { type: DataTypes.STRING(10), allowNull: false },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      ptrsId: { type: DataTypes.STRING(10), allowNull: false },
      stageRowId: { type: DataTypes.STRING(10), allowNull: false },
      rowNo: { type: DataTypes.INTEGER, allowNull: false },
      normalisationGroupKey: { type: DataTypes.TEXT, allowNull: true },
      normalisationRole: { type: DataTypes.STRING(30), allowNull: false },
      documentType: { type: DataTypes.STRING(50), allowNull: true },
      companyCode: { type: DataTypes.TEXT, allowNull: true },
      sourceAccountCode: { type: DataTypes.TEXT, allowNull: true },
      clearingDocument: { type: DataTypes.TEXT, allowNull: true },
      normalisationAmount: { type: DataTypes.DECIMAL, allowNull: false },
      originalObligationAmount: { type: DataTypes.DECIMAL, allowNull: true },
      adjustedObligationAmount: { type: DataTypes.DECIMAL, allowNull: true },
      adjustmentAllocatedAmount: { type: DataTypes.DECIMAL, allowNull: true },
      paymentAllocatedAmount: { type: DataTypes.DECIMAL, allowNull: true },
      outstandingAmount: { type: DataTypes.DECIMAL, allowNull: true },
      unmatchedAmount: { type: DataTypes.DECIMAL, allowNull: true },
      reversalOffsetAmount: { type: DataTypes.DECIMAL, allowNull: true },
      exceptionCode: { type: DataTypes.STRING(64), allowNull: true },
    },
    {
      tableName: "tbl_ptrs_payment_normalisation_row",
      timestamps: true,
      updatedAt: false,
      paranoid: false,
      indexes: [
        {
          name: "ptrs_payment_normalisation_row_result_stage_ux",
          unique: true,
          fields: ["normalisationResultId", "stageRowId"],
        },
        {
          name: "ptrs_payment_normalisation_row_result_role_idx",
          fields: ["normalisationResultId", "normalisationRole"],
        },
        {
          name: "ptrs_payment_normalisation_row_result_group_idx",
          fields: ["normalisationResultId", "normalisationGroupKey"],
        },
      ],
    },
  );
}
