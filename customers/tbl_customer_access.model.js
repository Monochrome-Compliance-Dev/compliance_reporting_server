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
    userId: { type: DataTypes.STRING(10), allowNull: false },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    role: { type: DataTypes.STRING, allowNull: true }, // optional acting-as role override
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const CustomerAccess = sequelize.define("CustomerAccess", attributes, {
    tableName: "tbl_customer_access",
    timestamps: true,
    paranoid: true,
    indexes: [
      { unique: true, fields: ["userId", "customerId"] },
      { fields: ["customerId"] },
      { fields: ["userId"] },
    ],
  });

  return CustomerAccess;
}
