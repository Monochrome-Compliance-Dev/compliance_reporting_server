const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  return sequelize.define(
    "PtrsPaymentNormalisationException",
    {
      id: { type: DataTypes.BIGINT, autoIncrement: true, primaryKey: true },
      normalisationResultId: { type: DataTypes.STRING(10), allowNull: false },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      ptrsId: { type: DataTypes.STRING(10), allowNull: false },
      sourceStageRowId: { type: DataTypes.STRING(10), allowNull: false },
      reasonCode: { type: DataTypes.STRING(64), allowNull: false },
      amount: { type: DataTypes.DECIMAL, allowNull: false },
      documentType: { type: DataTypes.STRING(50), allowNull: true },
    },
    {
      tableName: "tbl_ptrs_payment_normalisation_exception",
      timestamps: true,
      updatedAt: false,
      paranoid: false,
      indexes: [
        {
          name: "ptrs_payment_normalisation_exception_result_reason_idx",
          fields: ["normalisationResultId", "reasonCode"],
        },
        {
          name: "ptrs_payment_normalisation_exception_result_source_idx",
          fields: ["normalisationResultId", "sourceStageRowId"],
        },
      ],
    },
  );
}
