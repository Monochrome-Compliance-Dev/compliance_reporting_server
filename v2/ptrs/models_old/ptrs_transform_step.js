const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: An ordered step within a recipe.
 * `kind` is a string with validation to avoid tight-coupling to a DB ENUM at this stage.
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    recipeId: { type: DataTypes.STRING(10), allowNull: false },
    seq: { type: DataTypes.INTEGER, allowNull: false },
    kind: {
      type: DataTypes.STRING(30),
      allowNull: false,
      validate: {
        isIn: [
          [
            "filter", // WHERE-like conditions
            "derive", // computed column
            "rename", // rename a field
            "cast", // change type/format
            "join_lookup", // join to a lookup table
          ],
        ],
      },
    },
    config: { type: DataTypes.JSONB, allowNull: false }, // opaque to DB, interpreted by compiler
  };

  const PtrsTransformStep = sequelize.define(
    "ptrs_transform_step",
    attributes,
    {
      tableName: "tbl_ptrs_transform_step",
      timestamps: true,
      paranoid: false,
      indexes: [
        {
          name: "ux_ptrs_transform_step_recipe_seq",
          unique: true,
          fields: ["recipeId", "seq"],
        },
      ],
    }
  );

  return PtrsTransformStep;
}
