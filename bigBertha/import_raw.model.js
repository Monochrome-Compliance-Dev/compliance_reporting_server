const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

// Staging table for raw CSV rows before normalization/validation.
// Intentionally keeps MOST columns as strings so we don't fight casting
// (the worker normalizes and casts into final tables).
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },

    // Ingest/job meta
    jobId: { type: DataTypes.STRING(24), allowNull: false },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    ptrsId: { type: DataTypes.STRING(10), allowNull: false },
    rowNumber: { type: DataTypes.INTEGER, allowNull: true },

    // Business columns (store as text in staging)
    payerEntityName: { type: DataTypes.STRING, allowNull: true },
    payerEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payerEntityAcnArbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityName: { type: DataTypes.STRING, allowNull: true },
    payeeEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityAcnArbn: { type: DataTypes.STRING, allowNull: true },
    paymentAmount: { type: DataTypes.STRING, allowNull: true },
    description: { type: DataTypes.TEXT, allowNull: true },
    transactionType: { type: DataTypes.STRING, allowNull: true },
    isReconciled: { type: DataTypes.STRING, allowNull: true },
    supplyDate: { type: DataTypes.STRING, allowNull: true },
    paymentDate: { type: DataTypes.STRING, allowNull: true },
    contractPoReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    contractPoPaymentTerms: { type: DataTypes.STRING, allowNull: true },
    noticeForPaymentIssueDate: { type: DataTypes.STRING, allowNull: true },
    noticeForPaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    invoiceIssueDate: { type: DataTypes.STRING, allowNull: true },
    invoiceReceiptDate: { type: DataTypes.STRING, allowNull: true },
    invoiceAmount: { type: DataTypes.STRING, allowNull: true },
    invoicePaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceDueDate: { type: DataTypes.STRING, allowNull: true },
    accountCode: { type: DataTypes.STRING, allowNull: true },
    isTcp: { type: DataTypes.STRING, allowNull: true },
    tcpExclusionComment: { type: DataTypes.TEXT, allowNull: true },
    peppolEnabled: { type: DataTypes.STRING, allowNull: true },
    rcti: { type: DataTypes.STRING, allowNull: true },
    creditCardPayment: { type: DataTypes.STRING, allowNull: true },
    creditCardNumber: { type: DataTypes.STRING, allowNull: true },
    partialPayment: { type: DataTypes.STRING, allowNull: true },
    paymentTerm: { type: DataTypes.STRING, allowNull: true },
    excludedTcp: { type: DataTypes.STRING, allowNull: true },
    explanatoryComments1: { type: DataTypes.TEXT, allowNull: true },
    isSb: { type: DataTypes.STRING, allowNull: true },
    paymentTime: { type: DataTypes.STRING, allowNull: true },
    explanatoryComments2: { type: DataTypes.TEXT, allowNull: true },

    // Traceability (who/what created these rows)
    source: { type: DataTypes.STRING(20), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const ImportRaw = sequelize.define("ImportRaw", attributes, {
    tableName: "tbl_tcp_import_raw",
    timestamps: true,
    paranoid: true, // soft-deletes via deletedAt
    indexes: [
      { unique: false, fields: ["jobId"] },
      { unique: false, fields: ["customerId", "ptrsId"] },
      { unique: false, fields: ["ptrsId", "rowNumber"] },
    ],
  });

  ImportRaw.associate = (models) => {
    if (models.Customer) {
      ImportRaw.belongsTo(models.Customer, { foreignKey: "customerId" });
      models.Customer.hasMany(ImportRaw, {
        foreignKey: "customerId",
        onDelete: "CASCADE",
      });
    }
    if (models.Report || models.Ptrs) {
      const ReportModel = models.Report || models.Ptrs;
      ImportRaw.belongsTo(ReportModel, {
        foreignKey: "ptrsId",
        onDelete: "CASCADE",
      });
      ReportModel.hasMany(ImportRaw, {
        foreignKey: "ptrsId",
        onDelete: "CASCADE",
      });
    }
  };

  return ImportRaw;
}
