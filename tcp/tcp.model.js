const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
    payerEntityName: { type: DataTypes.STRING, allowNull: false },
    payerEntityAbn: { type: DataTypes.BIGINT, allowNull: true }, // Changed to BIGINT for numbers
    payerEntityAcnArbn: { type: DataTypes.BIGINT, allowNull: true }, // Changed to BIGINT for numbers
    payeeEntityName: { type: DataTypes.STRING, allowNull: false },
    payeeEntityAbn: { type: DataTypes.BIGINT, allowNull: true }, // Changed to BIGINT for numbers
    payeeEntityAcnArbn: { type: DataTypes.BIGINT, allowNull: true }, // Changed to BIGINT for numbers
    paymentAmount: { type: DataTypes.DECIMAL(15, 2), allowNull: false }, // Changed to DECIMAL for monetary values
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
    invoiceAmount: { type: DataTypes.DECIMAL(15, 2), allowNull: true }, // Changed to DECIMAL for monetary values
    invoicePaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceDueDate: { type: DataTypes.DATE, allowNull: true },
    isTcp: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true }, // Field to track TCP selection
    tcpExclusionComment: { type: DataTypes.TEXT, allowNull: true }, // Field to store reviewer comments
    peppolEnabled: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: true,
    },
    rcti: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: false },
    creditCardPayment: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: false,
    },
    creditCardNumber: { type: DataTypes.STRING, allowNull: true },
    partialPayment: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: false,
    },
    paymentTerm: { type: DataTypes.INTEGER, allowNull: true },
    excludedTcp: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: false,
    },
    explanatoryComments1: { type: DataTypes.TEXT, allowNull: true },
    isSb: { type: DataTypes.BOOLEAN, allowNull: true, defaultValue: true },
    paymentTime: { type: DataTypes.INTEGER, allowNull: true },
    explanatoryComments2: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.INTEGER, allowNull: true },
    updatedBy: { type: DataTypes.INTEGER, allowNull: true },
  };

  return sequelize.define("tcp", attributes, { tableName: "tbl_tcp" });
}
