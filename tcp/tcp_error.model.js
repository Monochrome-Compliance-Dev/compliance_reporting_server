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
    payerEntityName: DataTypes.STRING,
    payerEntityAbn: DataTypes.STRING,
    payerEntityAcnArbn: DataTypes.STRING,
    payeeEntityName: DataTypes.STRING,
    payeeEntityAbn: DataTypes.STRING,
    payeeEntityAcnArbn: DataTypes.STRING,
    paymentAmount: DataTypes.DECIMAL,
    description: DataTypes.STRING,
    transactionType: DataTypes.STRING,
    isReconciled: DataTypes.BOOLEAN,
    supplyDate: DataTypes.DATE,
    paymentDate: DataTypes.DATE,
    contractPoReferenceNumber: DataTypes.STRING,
    contractPoPaymentTerms: DataTypes.STRING,
    noticeForPaymentIssueDate: DataTypes.DATE,
    noticeForPaymentTerms: DataTypes.STRING,
    invoiceReferenceNumber: DataTypes.STRING,
    invoiceIssueDate: DataTypes.DATE,
    invoiceReceiptDate: DataTypes.DATE,
    invoiceAmount: DataTypes.DECIMAL,
    invoicePaymentTerms: DataTypes.STRING,
    invoiceDueDate: DataTypes.DATE,
    errorReason: DataTypes.JSONB,
    source: { type: DataTypes.STRING(20), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    // Foreign keys
    clientId: { type: DataTypes.STRING(10), allowNull: false },
    reportId: { type: DataTypes.STRING(10), allowNull: false },
  };

  const TcpError = sequelize.define("TcpError", attributes, {
    tableName: "tbl_tcp_error",
    timestamps: true,
  });

  TcpError.associate = (models) => {
    TcpError.belongsTo(models.Client, { foreignKey: "clientId" });
    models.Client.hasMany(TcpError, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });

    TcpError.belongsTo(models.Report, {
      foreignKey: "reportId",
      onDelete: "CASCADE",
    });
    models.Report.hasMany(TcpError, {
      foreignKey: "reportId",
      onDelete: "CASCADE",
    });
  };

  return TcpError;
}
