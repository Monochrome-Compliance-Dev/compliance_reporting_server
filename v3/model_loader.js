const fs = require("fs");
const path = require("path");

function getModelFiles(dir) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  return entries.flatMap((entry) => {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      return getModelFiles(fullPath);
    }

    if (!entry.isFile()) {
      return [];
    }

    if (!entry.name.endsWith(".model.js")) {
      return [];
    }

    return [fullPath];
  });
}

function initV3Models(sequelize) {
  const models = {};
  const baseDir = __dirname;
  const files = getModelFiles(baseDir).filter(
    (filePath) => path.resolve(filePath) !== path.resolve(__filename),
  );

  for (const filePath of files) {
    const defineModel = require(filePath);

    if (typeof defineModel !== "function") {
      continue;
    }

    const model = defineModel(sequelize);

    if (model && model.name) {
      models[model.name] = model;
    }
  }

  Object.values(models).forEach((model) => {
    if (model && typeof model.associate === "function") {
      model.associate(models);
    }
  });

  return models;
}

module.exports = {
  initV3Models,
};
