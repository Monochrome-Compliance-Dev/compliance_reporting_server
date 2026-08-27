const fs = require("fs");
const path = require("path");

jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: jest.fn(() => "dataset001"),
}));

const {
  DATASET_PURPOSES,
  REFERENCE_KINDS,
  XERO_TRANSACTION_DATASET,
  validateDatasetClassification,
} = require("./datasets.ptrs.service");

function defineModel(factory) {
  const sequelize = {
    define: jest.fn((name, rawAttributes, options) => ({
      name,
      rawAttributes,
      options,
    })),
  };
  return factory(sequelize);
}

describe("PTRS dataset classification and schema", () => {
  test("classifies transaction datasets without filename or singleton role inference", () => {
    expect(
      validateDatasetClassification({
        purpose: "transaction",
        sourceFormat: "csv",
        adapterType: "sap_accounting_event",
        sourceGroupScope: "ledger-au",
      }),
    ).toEqual({
      purpose: DATASET_PURPOSES.TRANSACTION,
      sourceFormat: "csv",
      referenceKind: null,
      adapterType: "sap_accounting_event",
      adapterVersion: null,
      sourceGroupScope: "ledger-au",
      role: "transaction",
    });
  });

  test.each(REFERENCE_KINDS)(
    "classifies the %s reference kind independently",
    (referenceKind) => {
      expect(
        validateDatasetClassification({
          purpose: "reference",
          sourceFormat: "csv",
          referenceKind,
        }),
      ).toMatchObject({
        purpose: DATASET_PURPOSES.REFERENCE,
        referenceKind,
        role: referenceKind,
      });
    },
  );

  test("rejects incomplete or contradictory classification", () => {
    expect(() =>
      validateDatasetClassification({
        purpose: "reference",
        sourceFormat: "csv",
      }),
    ).toThrow("referenceKind is required");
    expect(() =>
      validateDatasetClassification({
        purpose: "transaction",
        sourceFormat: "csv",
        referenceKind: "vendormaster",
      }),
    ).toThrow("referenceKind must be empty");
  });

  test("defines Xero import as an explicit API transaction dataset", () => {
    expect(XERO_TRANSACTION_DATASET).toEqual({
      role: "transaction",
      purpose: "transaction",
      sourceFormat: "api",
      adapterType: "xero_accounting_event",
      sourceType: "xero",
    });

    const xeroSource = fs.readFileSync(
      path.join(__dirname, "../xero/xero.service.js"),
      "utf8",
    );
    expect(xeroSource).toContain("...XERO_TRANSACTION_DATASET");
    expect(xeroSource).not.toContain("upsertMainDatasetFromRaw");
  });

  test("model identity permits multiple transaction datasets and scopes canonical revisions and rows", () => {
    const dataset = defineModel(require("../models/ptrs_dataset"));
    const fieldMap = defineModel(require("../models/ptrs_field_map"));
    const canonicalRevision = defineModel(
      require("../models/ptrs_canonical_revision"),
    );
    const canonicalRow = defineModel(
      require("../models/ptrs_canonical_source_row"),
    );

    expect(dataset.rawAttributes.purpose.allowNull).toBe(false);
    expect(dataset.rawAttributes.sourceFormat.allowNull).toBe(false);
    expect(dataset.rawAttributes.referenceKind.allowNull).toBe(true);
    expect(
      dataset.options.indexes.some(
        (index) =>
          index.unique &&
          index.fields?.includes("purpose") &&
          index.fields?.includes("ptrsId"),
      ),
    ).toBe(false);

    const fieldMapIdentity = fieldMap.options.indexes.find(
      (index) => index.name === "ux_ptrs_field_map_canon",
    );
    expect(fieldMapIdentity.unique).toBe(true);
    expect(fieldMapIdentity.fields).toEqual([
      "customerId",
      "ptrsId",
      "profileId",
      "datasetId",
      "canonicalField",
    ]);

    expect(canonicalRevision.rawAttributes.datasetId.allowNull).toBe(false);
    expect(canonicalRevision.rawAttributes.materialSignature.allowNull).toBe(false);
    expect(canonicalRow.rawAttributes.canonicalRevisionId.allowNull).toBe(false);
    expect(canonicalRow.rawAttributes.semanticKind.allowNull).toBe(false);
    expect(
      canonicalRow.options.indexes.find((index) => index.unique)?.fields,
    ).toEqual(["customerId", "canonicalRevisionId", "sourceRowNo"]);
    expect(() =>
      canonicalRevision.options.hooks.beforeUpdate({
        previous: () => "succeeded",
      }),
    ).toThrow("Successful canonical revisions are immutable");
  });

  test("migration deterministically classifies current transaction and reference rows", () => {
    const migration = fs.readFileSync(
      path.join(
        __dirname,
        "../../../db/migrations/20260826_ptrs_multi_transaction_datasets.sql",
      ),
      "utf8",
    );

    expect(migration).toContain("THEN 'transaction'");
    expect(migration).toContain("THEN 'vendormaster'");
    expect(migration).toContain("THEN 'termschanges'");
    expect(migration).toContain("THEN 'entitystructure'");
    expect(migration).toContain("THEN 'invoices'");
    expect(migration).toContain("ELSE 'other'");
    expect(migration).toContain("THEN 'xero_accounting_event'");
    expect(migration).toContain(
      '"customerId", "ptrsId", "datasetId", "rowNo"',
    );
  });

  test("canonical migration requires rebuild and installs immutable-source provenance schema", () => {
    const migration = fs.readFileSync(
      path.join(
        __dirname,
        "../../../db/migrations/20260827_ptrs_canonical_revisions.sql",
      ),
      "utf8",
    );
    expect(migration).toContain('CREATE TABLE "tbl_ptrs_canonical_revision"');
    expect(migration).toContain('CREATE TABLE "tbl_ptrs_canonical_source_row"');
    expect(migration).toContain('TRUNCATE TABLE "tbl_ptrs_stage_row"');
    expect(migration).toContain('ADD COLUMN "canonicalRevisionId"');
    expect(migration).toContain('DROP TABLE "tbl_ptrs_mapped_row"');
    expect(migration).toContain("FORCE ROW LEVEL SECURITY");
  });
});
