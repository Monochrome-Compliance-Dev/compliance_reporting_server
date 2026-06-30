const {
  SCHEMA_STATUSES,
  SCHEMA_DATA_TYPES,
  SCHEMA_PARSER_TYPES,
} = require("../constants/schemaDefinition.constants");

const paymentSchemaDefinition = Object.freeze({
  schemaKey: "payment",
  name: "Payment Dataset",
  datasetType: "payment",
  version: 1,
  status: SCHEMA_STATUSES.DRAFT,
  description:
    "Reference schema definition for structured payment datasets used by the platform.",
  fields: [
    {
      name: "invoiceReference",
      sourceHeader: "Invoice Reference",
      aliases: ["Invoice Number", "Invoice No", "Invoice #", "Reference"],
      dataType: SCHEMA_DATA_TYPES.STRING,
      required: true,
      nullable: false,
      parser: SCHEMA_PARSER_TYPES.STRING,
      validation: [],
      examples: ["INV-10001", "10001"],
      modelHints: {
        storageType: "STRING",
        indexRecommended: true,
      },
      transformerHints: {
        trimWhitespace: true,
        emptyStringAsNull: true,
      },
    },
    {
      name: "paymentDate",
      sourceHeader: "Payment Date",
      aliases: ["Paid Date", "Date Paid", "Payment Dt"],
      dataType: SCHEMA_DATA_TYPES.DATE,
      required: true,
      nullable: false,
      parser: SCHEMA_PARSER_TYPES.DATE,
      validation: [],
      examples: ["2026-06-29", "29/06/2026"],
      modelHints: {
        storageType: "DATE",
        indexRecommended: true,
      },
      transformerHints: {
        trimWhitespace: true,
        emptyStringAsNull: true,
      },
    },
    {
      name: "paymentAmount",
      sourceHeader: "Payment Amount",
      aliases: ["Amount", "Paid Amount", "Payment Value"],
      dataType: SCHEMA_DATA_TYPES.DECIMAL,
      required: true,
      nullable: false,
      parser: SCHEMA_PARSER_TYPES.DECIMAL,
      validation: [],
      examples: ["1250.00", "$1,250.00", "-42.18"],
      modelHints: {
        storageType: "DECIMAL",
        precision: 18,
        scale: 2,
      },
      transformerHints: {
        trimWhitespace: true,
        emptyStringAsNull: true,
        removeCurrencySymbols: true,
        removeThousandsSeparators: true,
      },
    },
    {
      name: "payeeAbn",
      sourceHeader: "Payee ABN",
      aliases: ["Supplier ABN", "Vendor ABN", "ABN"],
      dataType: SCHEMA_DATA_TYPES.STRING,
      required: false,
      nullable: true,
      parser: SCHEMA_PARSER_TYPES.STRING,
      validation: [],
      examples: ["12345678901", "12 345 678 901"],
      modelHints: {
        storageType: "STRING",
        indexRecommended: true,
      },
      transformerHints: {
        trimWhitespace: true,
        emptyStringAsNull: true,
        removeSpaces: true,
      },
    },
    {
      name: "payeeName",
      sourceHeader: "Payee Name",
      aliases: ["Supplier Name", "Vendor Name"],
      dataType: SCHEMA_DATA_TYPES.STRING,
      required: false,
      nullable: true,
      parser: SCHEMA_PARSER_TYPES.STRING,
      validation: [],
      examples: ["Example Supplier Pty Ltd"],
      modelHints: {
        storageType: "STRING",
      },
      transformerHints: {
        trimWhitespace: true,
        emptyStringAsNull: true,
        normaliseRepeatedSpaces: true,
      },
    },
  ],
});

module.exports = paymentSchemaDefinition;
