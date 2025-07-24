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
    name: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    code: {
      type: DataTypes.STRING,
      allowNull: false,
      unique: true,
    },
    type: {
      type: DataTypes.ENUM("solution", "module", "addon"),
      allowNull: false,
    },
    amount: {
      type: DataTypes.DECIMAL(10, 2),
      allowNull: false,
    },
    active: {
      type: DataTypes.BOOLEAN,
      defaultValue: true,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },
  };

  const Product = sequelize.define("product", attributes, {
    tableName: "tbl_product",
    timestamps: true,
    paranoid: true,
  });

  Product.associate = (models) => {
    Product.hasMany(models.InvoiceLine, { foreignKey: "productId" });
  };

  return Product;
}
