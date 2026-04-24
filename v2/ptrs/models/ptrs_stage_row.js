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
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    datasetId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    rowNo: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    payerEntityName: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    payerEntityAbn: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    payeeEntityName: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    payeeEntityAbn: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    invoiceReferenceNumber: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    sourceAccountCode: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    description: {
      type: DataTypes.TEXT,
      allowNull: true,
    },
    documentType: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    clearingDocument: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    paymentAmount: {
      type: DataTypes.DECIMAL(18, 2),
      allowNull: true,
    },
    paymentDate: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    invoiceIssueDate: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    invoiceReceiptDate: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    invoiceDueDate: {
      type: DataTypes.DATEONLY,
      allowNull: true,
    },
    paymentTermRaw: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    paymentTermDays: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },
    paymentTimeDays: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },
    tradeCreditPayment: {
      type: DataTypes.BOOLEAN,
      allowNull: true,
    },
    excludedTradeCreditPayment: {
      type: DataTypes.BOOLEAN,
      allowNull: true,
    },
    excludeReason: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    data: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    errors: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
  };

  const PtrsStageRow = sequelize.define("PtrsStageRow", attributes, {
    tableName: "tbl_ptrs_stage_row",
    timestamps: true,
    paranoid: true,
    indexes: [
      { name: "ptrs_stage_row_customer_id_idx", fields: ["customerId"] },
      { name: "ptrs_stage_row_ptrs_id_idx", fields: ["ptrsId"] },
      { name: "ptrs_stage_row_profile_id_idx", fields: ["profileId"] },
      { name: "ptrs_stage_row_dataset_id_idx", fields: ["datasetId"] },
      {
        name: "ptrs_stage_row_customer_ptrs_idx",
        fields: ["customerId", "ptrsId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_idx",
        fields: ["customerId", "ptrsId", "profileId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_document_type_idx",
        fields: ["customerId", "ptrsId", "profileId", "documentType"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_clearing_document_idx",
        fields: ["customerId", "ptrsId", "profileId", "clearingDocument"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_payment_time_days_idx",
        fields: ["customerId", "ptrsId", "profileId", "paymentTimeDays"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_trade_credit_idx",
        fields: ["customerId", "ptrsId", "profileId", "tradeCreditPayment"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_excluded_tcp_idx",
        fields: [
          "customerId",
          "ptrsId",
          "profileId",
          "excludedTradeCreditPayment",
        ],
      },
      {
        name: "ptrs_stage_row_ptrs_dataset_idx",
        fields: ["ptrsId", "datasetId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_dataset_idx",
        fields: ["customerId", "ptrsId", "datasetId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_invoice_ref_idx",
        fields: ["customerId", "ptrsId", "profileId", "invoiceReferenceNumber"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_payee_abn_idx",
        fields: ["customerId", "ptrsId", "profileId", "payeeEntityAbn"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_source_account_idx",
        fields: ["customerId", "ptrsId", "profileId", "sourceAccountCode"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_description_idx",
        fields: ["customerId", "ptrsId", "profileId", "description"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_dataset_rowno_idx",
        fields: ["customerId", "ptrsId", "profileId", "datasetId", "rowNo"],
      },
    ],
  });

  return PtrsStageRow;
}
