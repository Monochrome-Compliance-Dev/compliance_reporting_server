const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: A reusable transformation recipe (ordered steps live in ptrs_transform_step).
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    name: { type: DataTypes.STRING, allowNull: false },
    description: { type: DataTypes.TEXT, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const PtrsRecipe = sequelize.define("ptrs_recipe", attributes, {
    tableName: "tbl_ptrs_recipe",
    timestamps: true,
    paranoid: true, // allow soft-delete so users can hide old recipes
    indexes: [
      { name: "idx_ptrs_recipe_customer_name", fields: ["customerId", "name"] },
    ],
  });

  return PtrsRecipe;
}
