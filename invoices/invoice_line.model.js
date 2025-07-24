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
    invoiceId: {
      type: DataTypes.STRING(10),
      allowNull: false,
      references: {
        model: "tbl_invoice",
        key: "id",
      },
    },
    description: {
      type: DataTypes.TEXT,
      allowNull: false,
    },
    amount: {
      type: DataTypes.DECIMAL(10, 2),
      allowNull: false,
    },
    module: {
      type: DataTypes.ENUM(
        "ptrs",
        "esg",
        "training",
        "audit",
        "grievance",
        "ms"
      ),
      allowNull: false,
    },
    relatedRecordId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
    productId: {
      type: DataTypes.STRING(10),
      allowNull: false,
      references: {
        model: "tbl_product",
        key: "id",
      },
    },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const InvoiceLine = sequelize.define("invoice_line", attributes, {
    tableName: "tbl_invoice_line",
    timestamps: true,
    paranoid: true,
  });

  InvoiceLine.associate = (models) => {
    InvoiceLine.belongsTo(models.Invoice, { foreignKey: "invoiceId" });
    InvoiceLine.belongsTo(models.Product, { foreignKey: "productId" });
  };

  return InvoiceLine;
}
