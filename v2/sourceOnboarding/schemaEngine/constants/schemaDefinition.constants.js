const SCHEMA_STATUSES = Object.freeze({
  DRAFT: "Draft",
  APPROVED: "Approved",
  DEPRECATED: "Deprecated",
});

const SCHEMA_DATA_TYPES = Object.freeze({
  STRING: "string",
  INTEGER: "integer",
  DECIMAL: "decimal",
  BOOLEAN: "boolean",
  DATE: "date",
  DATETIME: "datetime",
  JSON: "json",
});

const SCHEMA_PARSER_TYPES = Object.freeze({
  STRING: "string",
  INTEGER: "integer",
  DECIMAL: "decimal",
  BOOLEAN: "boolean",
  DATE: "date",
  DATETIME: "datetime",
  JSON: "json",
});

const SCHEMA_STATUS_VALUES = Object.freeze(Object.values(SCHEMA_STATUSES));
const SCHEMA_DATA_TYPE_VALUES = Object.freeze(Object.values(SCHEMA_DATA_TYPES));
const SCHEMA_PARSER_TYPE_VALUES = Object.freeze(
  Object.values(SCHEMA_PARSER_TYPES),
);

module.exports = {
  SCHEMA_STATUSES,
  SCHEMA_DATA_TYPES,
  SCHEMA_PARSER_TYPES,
  SCHEMA_STATUS_VALUES,
  SCHEMA_DATA_TYPE_VALUES,
  SCHEMA_PARSER_TYPE_VALUES,
};
