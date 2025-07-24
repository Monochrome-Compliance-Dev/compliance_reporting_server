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
    billingType: {
      type: DataTypes.ENUM("DIRECT", "PARTNER"),
      allowNull: false,
    },
    clientId: {
      type: DataTypes.STRING(10),
      allowNull: true,
      references: {
        model: "tbl_client",
        key: "id",
      },
    },
    partnerId: {
      type: DataTypes.STRING(10),
      allowNull: true,
      references: {
        model: "tbl_partner",
        key: "id",
      },
    },
    reportingPeriodId: {
      type: DataTypes.STRING(20),
      allowNull: false,
    },
    issuedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    totalAmount: {
      type: DataTypes.DECIMAL(10, 2),
      allowNull: false,
    },
    status: {
      type: DataTypes.ENUM("draft", "issued", "paid", "cancelled"),
      allowNull: false,
      defaultValue: "draft",
    },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Invoice = sequelize.define("invoice", attributes, {
    tableName: "tbl_invoice",
    timestamps: true,
    paranoid: true,
  });

  Invoice.associate = (models) => {
    Invoice.belongsTo(models.Client, { foreignKey: "clientId" });
    Invoice.belongsTo(models.Partner, { foreignKey: "partnerId" });
    Invoice.hasMany(models.InvoiceLine, {
      foreignKey: "invoiceId",
      onDelete: "CASCADE",
    });
  };

  return Invoice;
}
