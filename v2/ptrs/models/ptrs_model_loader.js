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

  // Run model-level associations if provided (e.g., ptrs_profile ↔ ptrs_customer_profile)
  Object.values(models).forEach((m) => {
    if (m && typeof m.associate === "function") {
      m.associate(models);
    }
  });

  // --- Associations for PTRS v2 models ---

  // PTRS 1<->N datasets
  if (models.ptrs && models.ptrs_dataset) {
    models.ptrs_dataset.belongsTo(models.ptrs, {
      foreignKey: "ptrsId",
      as: "ptrs",
    });
    models.ptrs.hasMany(models.ptrs_dataset, {
      foreignKey: "ptrsId",
      as: "datasets",
    });
  }

  // PTRS 1<->1 column map
  if (models.ptrs && models.ptrs_column_map) {
    models.ptrs_column_map.belongsTo(models.ptrs, {
      foreignKey: "ptrsId",
      as: "ptrs",
    });
    models.ptrs.hasOne(models.ptrs_column_map, {
      foreignKey: "ptrsId",
      as: "columnMap",
    });
  }

  // Profile 1<->N PTRS instances
  if (models.ptrs_profile && models.ptrs) {
    models.ptrs.belongsTo(models.ptrs_profile, {
      foreignKey: "profileId",
      as: "profile",
    });
    models.ptrs_profile.hasMany(models.ptrs, {
      foreignKey: "profileId",
      as: "ptrsList",
    });
  }

  // Profile 1<->N column maps
  if (models.ptrs_profile && models.ptrs_column_map) {
    models.ptrs_column_map.belongsTo(models.ptrs_profile, {
      foreignKey: "profileId",
      as: "profile",
    });
    models.ptrs_profile.hasMany(models.ptrs_column_map, {
      foreignKey: "profileId",
      as: "columnMaps",
    });
  }

  return models;
}

module.exports = {
  initPtrsV2Models,
};
