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
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    ptrsId: { type: DataTypes.STRING(10), allowNull: false },
    canonicalRevisionId: { type: DataTypes.STRING(10), allowNull: false },
    datasetId: { type: DataTypes.STRING(10), allowNull: false },
    sourceRawRowId: { type: DataTypes.STRING(10), allowNull: true },
    sourceRowNo: { type: DataTypes.INTEGER, allowNull: false },
    sourceGroupScope: { type: DataTypes.STRING(100), allowNull: true },
    adapterType: { type: DataTypes.STRING(50), allowNull: false },
    adapterVersion: { type: DataTypes.STRING(30), allowNull: true },
    semanticKind: {
      type: DataTypes.STRING(30),
      allowNull: false,
      validate: { isIn: [["accounting_event", "direct_payment"]] },
    },
    payerEntityName: { type: DataTypes.STRING, allowNull: true },
    payerEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityName: { type: DataTypes.STRING, allowNull: true },
    payeeEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityAbnValid: { type: DataTypes.BOOLEAN, allowNull: true },
    invoiceReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    sourceAccountCode: { type: DataTypes.STRING, allowNull: true },
    description: { type: DataTypes.TEXT, allowNull: true },
    documentType: { type: DataTypes.STRING, allowNull: true },
    documentCurrency: { type: DataTypes.STRING, allowNull: true },
    clearingDocument: { type: DataTypes.STRING, allowNull: true },
    reconciliationStatus: { type: DataTypes.STRING, allowNull: true },
    sourceUser: { type: DataTypes.STRING, allowNull: true },
    paymentAmount: { type: DataTypes.DECIMAL(18, 2), allowNull: true },
    paymentDate: { type: DataTypes.DATEONLY, allowNull: true },
    invoiceIssueDate: { type: DataTypes.DATEONLY, allowNull: true },
    invoiceReceiptDate: { type: DataTypes.DATEONLY, allowNull: true },
    invoiceDueDate: { type: DataTypes.DATEONLY, allowNull: true },
    invoiceCreatedDate: { type: DataTypes.DATEONLY, allowNull: true },
    entryDate: { type: DataTypes.DATEONLY, allowNull: true },
    paymentTermRaw: { type: DataTypes.STRING, allowNull: true },
    data: { type: DataTypes.JSONB, allowNull: false },
    provenance: { type: DataTypes.JSONB, allowNull: false },
  };

  return sequelize.define("PtrsCanonicalSourceRow", attributes, {
    tableName: "tbl_ptrs_canonical_source_row",
    timestamps: true,
    updatedAt: false,
    paranoid: false,
    indexes: [
      {
        name: "ptrs_canon_src_customer_idx",
        fields: ["customerId"],
      },
      {
        name: "ptrs_canon_src_ptrs_idx",
        fields: ["ptrsId"],
      },
      {
        name: "ptrs_canon_src_dataset_idx",
        fields: ["datasetId"],
      },
      {
        name: "ptrs_canon_src_revision_idx",
        fields: ["canonicalRevisionId"],
      },
      {
        name: "ptrs_canon_src_revision_row_ux",
        unique: true,
        fields: ["customerId", "canonicalRevisionId", "sourceRowNo"],
      },
      {
        name: "ptrs_canon_src_revision_order_idx",
        fields: ["canonicalRevisionId", "sourceRowNo", "id"],
      },
    ],
  });
}
