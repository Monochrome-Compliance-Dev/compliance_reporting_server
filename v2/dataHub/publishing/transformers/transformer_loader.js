const paymentTransformer = require("./payment.transformer");

const TRANSFORMERS = {
  [paymentTransformer.datasetType]: paymentTransformer,
};

function getTransformer(datasetType) {
  const transformer = TRANSFORMERS[datasetType];
  if (!transformer) {
    throw new Error(
      `Publishing transformer is not implemented for ${datasetType}`,
    );
  }

  if (!transformer.modelName) {
    throw new Error(
      `Publishing transformer for ${datasetType} is missing modelName`,
    );
  }

  if (typeof transformer.buildRows !== "function") {
    throw new Error(
      `Publishing transformer for ${datasetType} is missing buildRows`,
    );
  }

  return transformer;
}

module.exports = {
  getTransformer,
};
