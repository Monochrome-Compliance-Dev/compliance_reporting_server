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

  // Link upload -> column map (1:1)
  if (models.ptrs_upload && models.ptrs_column_map) {
    models.ptrs_column_map.belongsTo(models.ptrs_upload, {
      foreignKey: "runId",
      targetKey: "id",
      as: "run",
    });
    models.ptrs_upload.hasOne(models.ptrs_column_map, {
      foreignKey: "runId",
      sourceKey: "id",
      as: "columnMap",
    });
  }

  // Link upload -> import_raw (1:N)
  if (models.ptrs_upload && models.ptrs_import_raw) {
    models.ptrs_import_raw.belongsTo(models.ptrs_upload, {
      foreignKey: "runId",
      targetKey: "id",
      as: "run",
    });
    models.ptrs_upload.hasMany(models.ptrs_import_raw, {
      foreignKey: "runId",
      sourceKey: "id",
      as: "rawRows",
    });
  }

  // Link upload -> norm_row (1:N)
  if (models.ptrs_upload && models.ptrs_norm_row) {
    models.ptrs_norm_row.belongsTo(models.ptrs_upload, {
      foreignKey: "runId",
      targetKey: "id",
      as: "run",
    });
    models.ptrs_upload.hasMany(models.ptrs_norm_row, {
      foreignKey: "runId",
      sourceKey: "id",
      as: "normRows",
    });
  }

  return models;
}

module.exports = {
  initPtrsV2Models,
};
