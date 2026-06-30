const {
  SCHEMA_STATUS_VALUES,
  SCHEMA_DATA_TYPE_VALUES,
  SCHEMA_PARSER_TYPE_VALUES,
} = require("../constants/schemaDefinition.constants");

function validateSchemaDefinition(definition) {
  const errors = [];

  if (!definition || typeof definition !== "object") {
    return ["A schema definition payload is required."];
  }

  validateRequired(definition, "schemaKey", errors);
  validateRequired(definition, "name", errors);
  validateRequired(definition, "datasetType", errors);
  validateRequired(definition, "version", errors);
  validateRequired(definition, "status", errors);

  if (!SCHEMA_STATUS_VALUES.includes(definition.status)) {
    errors.push(`Unsupported schema status: ${definition.status}`);
  }

  if (!Array.isArray(definition.fields) || definition.fields.length === 0) {
    errors.push("A schema definition must contain at least one field.");
    return errors;
  }

  const fieldNames = new Set();
  const aliases = new Set();

  definition.fields.forEach((field, index) => {
    const prefix = `Field ${index + 1}`;

    validateRequired(field, "name", errors, prefix);
    validateRequired(field, "sourceHeader", errors, prefix);
    validateRequired(field, "dataType", errors, prefix);
    validateRequired(field, "parser", errors, prefix);

    if (!SCHEMA_DATA_TYPE_VALUES.includes(field.dataType)) {
      errors.push(`${prefix}: unsupported data type '${field.dataType}'.`);
    }

    if (!SCHEMA_PARSER_TYPE_VALUES.includes(field.parser)) {
      errors.push(`${prefix}: unsupported parser '${field.parser}'.`);
    }

    if (fieldNames.has(field.name)) {
      errors.push(`Duplicate field name '${field.name}'.`);
    } else {
      fieldNames.add(field.name);
    }

    if (Array.isArray(field.aliases)) {
      field.aliases.forEach((alias) => {
        if (aliases.has(alias)) {
          errors.push(`Duplicate alias '${alias}'.`);
        } else {
          aliases.add(alias);
        }
      });
    }
  });

  return errors;
}

function validateRequired(object, property, errors, prefix = "Schema") {
  if (
    object[property] === undefined ||
    object[property] === null ||
    object[property] === ""
  ) {
    errors.push(`${prefix}: '${property}' is required.`);
  }
}

module.exports = {
  validateSchemaDefinition,
};
