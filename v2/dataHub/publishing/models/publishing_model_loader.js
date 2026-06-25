const fs = require("fs");
const path = require("path");

/**
 * Initialize all Data Hub publishing models in this folder using CamelCase
 * model names exactly as defined inside each model file.
 */
function initDataHubPublishingModels(sequelize) {
  const models = {};
  const dir = __dirname;

  const files = fs.readdirSync(dir).filter((f) => {
    if (f === "publishing_model_loader.js") return false;
    return f.endsWith(".js");
  });

  for (const file of files) {
    const define = require(path.join(dir, file));
    if (typeof define === "function") {
      const model = define(sequelize);

      if (model && model.name) {
        models[model.name] = model;
      }
    }
  }

  Object.values(models).forEach((m) => {
    if (m && typeof m.associate === "function") {
      m.associate(models);
    }
  });

  return models;
}

module.exports = {
  initDataHubPublishingModels,
};
