const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    InvoiceNumber: { type: DataTypes.STRING, allowNull: false },
    InvoiceDate: { type: DataTypes.DATE, allowNull: false },
    DueDate: { type: DataTypes.DATE, allowNull: false },
    Amount: { type: DataTypes.FLOAT, allowNull: false },
    Status: { type: DataTypes.STRING, allowNull: false },
    created: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    updated: { type: DataTypes.DATE },
  };

  return sequelize.define("invoice", attributes, { tableName: "tbl_invoices" });
}
