const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  return sequelize.define(
    "PtrsPaymentNormalisationAllocation",
    {
      id: { type: DataTypes.BIGINT, autoIncrement: true, primaryKey: true },
      normalisationResultId: { type: DataTypes.STRING(10), allowNull: false },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      ptrsId: { type: DataTypes.STRING(10), allowNull: false },
      invoiceStageRowId: { type: DataTypes.STRING(10), allowNull: false },
      paymentStageRowId: { type: DataTypes.STRING(10), allowNull: false },
      paymentSequence: { type: DataTypes.BIGINT, allowNull: false },
      normalisationGroupKey: { type: DataTypes.TEXT, allowNull: false },
      companyCode: { type: DataTypes.TEXT, allowNull: true },
      sourceAccountCode: { type: DataTypes.TEXT, allowNull: true },
      clearingDocument: { type: DataTypes.TEXT, allowNull: true },
      allocatedAmount: { type: DataTypes.DECIMAL, allowNull: false },
      originalObligationAmount: { type: DataTypes.DECIMAL, allowNull: false },
      adjustedObligationAmount: { type: DataTypes.DECIMAL, allowNull: false },
      adjustmentAllocatedAmount: { type: DataTypes.DECIMAL, allowNull: false },
      settlementPaymentDate: { type: DataTypes.DATEONLY, allowNull: true },
      sourcePaymentAmount: { type: DataTypes.DECIMAL, allowNull: false },
      obligationBeforePayment: { type: DataTypes.DECIMAL, allowNull: false },
      obligationAfterPayment: { type: DataTypes.DECIMAL, allowNull: false },
      partialPayment: { type: DataTypes.BOOLEAN, allowNull: false },
      finalSettlement: { type: DataTypes.BOOLEAN, allowNull: false },
      paymentTimeDays: { type: DataTypes.INTEGER, allowNull: true },
      paymentTimeReferenceKind: { type: DataTypes.STRING(40), allowNull: true },
      paymentTimeReferenceDate: { type: DataTypes.DATEONLY, allowNull: true },
      paymentTimeReferencePolicy: {
        type: DataTypes.STRING(100),
        allowNull: true,
      },
      paymentTimeReferenceReason: { type: DataTypes.TEXT, allowNull: true },
      mappingExceptionCode: { type: DataTypes.STRING(64), allowNull: true },
    },
    {
      tableName: "tbl_ptrs_payment_normalisation_allocation",
      timestamps: true,
      updatedAt: false,
      paranoid: false,
      indexes: [
        {
          name: "ptrs_payment_normalisation_allocation_pair_ux",
          unique: true,
          fields: [
            "normalisationResultId",
            "invoiceStageRowId",
            "paymentStageRowId",
          ],
        },
        {
          name: "ptrs_payment_normalisation_allocation_result_payment_idx",
          fields: ["normalisationResultId", "paymentStageRowId"],
        },
        {
          name: "ptrs_payment_normalisation_allocation_result_invoice_idx",
          fields: ["normalisationResultId", "invoiceStageRowId"],
        },
      ],
    },
  );
}
