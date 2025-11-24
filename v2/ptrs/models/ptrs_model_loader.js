const fs = require("fs");
const path = require("path");

/**
 * Initialize all PTRS v2 models in this folder using CamelCase model names exactly as
 * defined inside each model file.
 */
function initPtrsV2Models(sequelize) {
  const models = {};
  const dir = __dirname;

  // Load *.js files except loader + old_models folder
  const files = fs.readdirSync(dir).filter((f) => {
    if (f === "ptrs_model_loader.js") return false;
    if (f === "old_models") return false;
    return f.endsWith(".js");
  });

  for (const file of files) {
    const define = require(path.join(dir, file));
    if (typeof define === "function") {
      const model = define(sequelize);

      // Use the model.name EXACTLY as declared in the model
      if (model && model.name) {
        models[model.name] = model;
      }
    }
  }

  // --- Run model-level associations ---
  Object.values(models).forEach((m) => {
    if (m && typeof m.associate === "function") {
      m.associate(models);
    }
  });

  // --- Associations using CamelCase model names ---

  if (models.Ptrs && models.PtrsDataset) {
    models.PtrsDataset.belongsTo(models.Ptrs, {
      foreignKey: "ptrsId",
      as: "ptrs",
    });
    models.Ptrs.hasMany(models.PtrsDataset, {
      foreignKey: "ptrsId",
      as: "datasets",
    });
  }

  if (models.Ptrs && models.PtrsColumnMap) {
    models.PtrsColumnMap.belongsTo(models.Ptrs, {
      foreignKey: "ptrsId",
      as: "ptrs",
    });
    models.Ptrs.hasOne(models.PtrsColumnMap, {
      foreignKey: "ptrsId",
      as: "columnMap",
    });
  }

  if (models.PtrsProfile && models.Ptrs) {
    models.Ptrs.belongsTo(models.PtrsProfile, {
      foreignKey: "profileId",
      as: "profile",
    });
    models.PtrsProfile.hasMany(models.Ptrs, {
      foreignKey: "profileId",
      as: "ptrsList",
    });
  }

  if (models.PtrsProfile && models.PtrsColumnMap) {
    models.PtrsColumnMap.belongsTo(models.PtrsProfile, {
      foreignKey: "profileId",
      as: "profile",
    });
    models.PtrsProfile.hasMany(models.PtrsColumnMap, {
      foreignKey: "profileId",
      as: "columnMaps",
    });
  }

  return models;
}

module.exports = {
  initPtrsV2Models,
};
