const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
    payerEntityName: { type: DataTypes.STRING, allowNull: false },
    payerEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payerEntityAcnArbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityName: { type: DataTypes.STRING, allowNull: false },
    payeeEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityAcnArbn: { type: DataTypes.STRING, allowNull: true },
    paymentAmount: { type: DataTypes.STRING, allowNull: false },
    description: { type: DataTypes.STRING, allowNull: true },
    supplyDate: { type: DataTypes.DATE, allowNull: true },
    paymentDate: { type: DataTypes.DATE, allowNull: false },
    contractPoReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    contractPoPaymentTerms: { type: DataTypes.STRING, allowNull: true },
    noticeForPaymentIssueDate: { type: DataTypes.DATE, allowNull: true },
    noticeForPaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    invoiceIssueDate: { type: DataTypes.DATE, allowNull: true },
    invoiceReceiptDate: { type: DataTypes.DATE, allowNull: true },
    invoicePaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceDueDate: { type: DataTypes.DATE, allowNull: true },
    isTcp: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: false }, // Field to track TCP selection
    comment: { type: DataTypes.TEXT, allowNull: true }, // Field to store reviewer comments
    updatedBy: { type: DataTypes.INTEGER, allowNull: false },
  };

  return sequelize.define("ptrs", attributes, { tableName: "tbl_ptrs" });
}
