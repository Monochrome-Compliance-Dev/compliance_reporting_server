const fs = require("fs");
const path = require("path");

/**
 * Initialize all PTRS v2 models found in this folder and set up lightweight associations.
 * Usage:
 *   const { initPtrsV2Models } = require("./v2/ptrs/models/ptrs_model_loader");
 *   const models = initPtrsV2Models(sequelize);
 */
function initPtrsV2Models(sequelize) {
  const models = {};
  const dir = __dirname;

  // Load every .js file in this directory except this loader itself.
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".js") && f !== "ptrs_model_loader.js");

  for (const file of files) {
    const define = require(path.join(dir, file));
    if (typeof define === "function") {
      const model = define(sequelize);
      if (model && model.name) {
        models[model.name] = model;
      }
    }
  }

  // --- Associations (limited, add more as we introduce models) ---
  // Recipe 1<->N TransformStep
  if (models.ptrs_recipe && models.ptrs_transform_step) {
    models.ptrs_transform_step.belongsTo(models.ptrs_recipe, {
      foreignKey: "recipeId",
      as: "recipe",
    });
    models.ptrs_recipe.hasMany(models.ptrs_transform_step, {
      foreignKey: "recipeId",
      as: "steps",
    });
  }

  // Future: upload -> column_map / import_raw relationships can be added once those models are wired
  // (we'll deliberately keep it light until services/controllers rely on these).

  return models;
}

module.exports = {
  initPtrsV2Models,
};
