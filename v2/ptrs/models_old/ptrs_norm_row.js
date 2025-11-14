const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: Normalised row storage for metrics/reporting.
 * One record per source row after mapping/fallbacks/defaults/rules are applied.
 * Canonical fields are explicit columns for efficient SQL; everything else in `extras` JSONB.
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    runId: { type: DataTypes.STRING(10), allowNull: false },
    rowNo: { type: DataTypes.INTEGER, allowNull: false }, // source row number

    // Canonical fields (keep types SQL-friendly)
    paymentDate: { type: DataTypes.DATE, allowNull: true },
    paymentAmount: { type: DataTypes.DECIMAL(18, 2), allowNull: true },

    payerEntityName: { type: DataTypes.STRING, allowNull: true },
    payerEntityAbn: { type: DataTypes.STRING, allowNull: true },
    payeeEntityName: { type: DataTypes.STRING, allowNull: true },
    payeeEntityAbn: { type: DataTypes.STRING, allowNull: true },

    documentType: { type: DataTypes.STRING, allowNull: true },
    documentReference: { type: DataTypes.STRING, allowNull: true },

    supplyDate: { type: DataTypes.DATE, allowNull: true },
    invoiceIssueDate: { type: DataTypes.DATE, allowNull: true },
    invoiceReceiptDate: { type: DataTypes.DATE, allowNull: true },
    invoiceDueDate: { type: DataTypes.DATE, allowNull: true },
    paymentTermsDays: { type: DataTypes.INTEGER, allowNull: true },

    isSmallBusiness: { type: DataTypes.BOOLEAN, allowNull: true },

    // Catch-all for passthrough columns (aliased or original)
    extras: { type: DataTypes.JSONB, allowNull: true },
  };

  const PtrsNormRow = sequelize.define("ptrs_norm_row", attributes, {
    tableName: "tbl_ptrs_norm_row",
    timestamps: true,
    paranoid: false,
    indexes: [
      { name: "idx_ptrs_norm_row_run_row", fields: ["runId", "rowNo"] },
      { name: "idx_ptrs_norm_row_payment_date", fields: ["paymentDate"] },
      { name: "idx_ptrs_norm_row_doc_ref", fields: ["documentReference"] },
    ],
  });

  return PtrsNormRow;
}
