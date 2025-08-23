const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    payerEntityName: { type: DataTypes.STRING, allowNull: false },
    payerEntityAbn: { type: DataTypes.BIGINT, allowNull: true },
    payerEntityAcnArbn: { type: DataTypes.BIGINT, allowNull: true },
    payeeEntityName: { type: DataTypes.STRING, allowNull: false },
    payeeEntityAbn: { type: DataTypes.BIGINT, allowNull: true },
    payeeEntityAcnArbn: { type: DataTypes.BIGINT, allowNull: true },
    paymentAmount: { type: DataTypes.DECIMAL(15, 2), allowNull: false },
    description: { type: DataTypes.STRING, allowNull: true },
    transactionType: { type: DataTypes.STRING, allowNull: true },
    isReconciled: {
      type: DataTypes.BOOLEAN,
      allowNull: true,
      defaultValue: false,
    },
    supplyDate: { type: DataTypes.STRING, allowNull: true },
    paymentDate: { type: DataTypes.DATE, allowNull: false },
    contractPoReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    contractPoPaymentTerms: { type: DataTypes.STRING, allowNull: true },
    noticeForPaymentIssueDate: { type: DataTypes.STRING, allowNull: true },
    noticeForPaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceReferenceNumber: { type: DataTypes.STRING, allowNull: true },
    invoiceIssueDate: { type: DataTypes.DATE, allowNull: true },
    invoiceReceiptDate: { type: DataTypes.STRING, allowNull: true },
    invoiceAmount: { type: DataTypes.DECIMAL(15, 2), allowNull: true },
    invoicePaymentTerms: { type: DataTypes.STRING, allowNull: true },
    invoiceDueDate: { type: DataTypes.DATE, allowNull: true },
    accountCode: { type: DataTypes.STRING, allowNull: true },
    isTcp: { type: DataTypes.BOOLEAN, allowNull: false, defaultValue: true },
    tcpExclusionComment: { type: DataTypes.TEXT, allowNull: true },
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
    // Additional fields
    source: { type: DataTypes.STRING(20), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    // Foreign keys
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    ptrsId: { type: DataTypes.STRING(10), allowNull: false },
  };

  const Tcp = sequelize.define("tcp", attributes, {
    tableName: "tbl_tcp",
    timestamps: true,
  });

  Tcp.associate = (models) => {
    Tcp.belongsTo(models.Customer, { foreignKey: "customerId" });
    models.Customer.hasMany(Tcp, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });

    Tcp.belongsTo(models.Ptrs, {
      foreignKey: "ptrsId",
      onDelete: "CASCADE",
    });
    models.Ptrs.hasMany(Tcp, { foreignKey: "ptrsId", onDelete: "CASCADE" });

    Tcp.hasMany(models.Audit, { foreignKey: "tcpId", onDelete: "CASCADE" });
    models.Audit.belongsTo(Tcp, { foreignKey: "tcpId", onDelete: "CASCADE" });
  };

  return Tcp;
}
