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
      adapterVersion: "1",
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
          adapterVersion: "reference-v1",
        }),
      ).toMatchObject({
        purpose: DATASET_PURPOSES.REFERENCE,
        referenceKind,
        adapterVersion: "reference-v1",
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
    expect(() =>
      validateDatasetClassification({
        purpose: "transaction",
        sourceFormat: "csv",
      }),
    ).toThrow("adapterType must identify a supported transaction adapter");
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
    const datasetReportingEntity = defineModel(
      require("../models/tbl_ptrs_dataset_reporting_entity_snapshot"),
    );

    expect(dataset.rawAttributes.purpose.allowNull).toBe(false);
    expect(dataset.rawAttributes.sourceFormat.allowNull).toBe(false);
    expect(dataset.rawAttributes.referenceKind.allowNull).toBe(true);
    expect(dataset.rawAttributes.dateFormat.allowNull).toBe(true);
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
    expect(canonicalRevision.rawAttributes.materialSignature.allowNull).toBe(
      false,
    );
    expect(canonicalRow.rawAttributes.canonicalRevisionId.allowNull).toBe(
      false,
    );
    expect(canonicalRow.rawAttributes.semanticKind.allowNull).toBe(false);
    expect(
      canonicalRow.options.indexes.find((index) => index.unique)?.fields,
    ).toEqual(["customerId", "canonicalRevisionId", "sourceRowNo"]);
    expect(
      canonicalRow.options.indexes.find(
        (index) =>
          index.name === "tbl_ptrs_canonical_source_row_source_raw_row_id_idx",
      )?.fields,
    ).toEqual(["sourceRawRowId"]);
    expect(datasetReportingEntity.options.indexes).toEqual([
      {
        name: "ptrs_dataset_reporting_entity_customer_idx",
        fields: ["customerId"],
      },
      {
        name: "ptrs_dataset_reporting_entity_ptrs_idx",
        fields: ["ptrsId"],
      },
      {
        name: "ptrs_dataset_reporting_entity_dataset_ux",
        fields: ["datasetId"],
        unique: true,
      },
      {
        name: "ptrs_dataset_reporting_entity_scope_ux",
        fields: ["customerId", "ptrsId", "datasetId"],
        unique: true,
      },
      {
        name: "ptrs_dataset_reporting_entity_abn_idx",
        fields: ["abn"],
      },
    ]);
    expect(datasetReportingEntity.rawAttributes.abn.type.toString()).toBe(
      "TEXT",
    );
    expect(() =>
      canonicalRevision.options.hooks.beforeUpdate({
        previous: () => "succeeded",
      }),
    ).toThrow("Successful canonical revisions are immutable");
  });

  test("direct-payment governance migration owns dataset identity, date format and raw-row lookup index", () => {
    const migration = fs.readFileSync(
      path.join(
        __dirname,
        "../../../db/migrations/20260914_ptrs_direct_payment_dataset_governance.sql",
      ),
      "utf8",
    );

    expect(migration).toContain(
      'ADD COLUMN IF NOT EXISTS "dateFormat" varchar(10)',
    );
    expect(migration).toContain("tbl_ptrs_dataset_reporting_entity_snapshot");
    expect(migration).toContain(
      "tbl_ptrs_canonical_source_row_source_raw_row_id_idx",
    );
    expect(migration).toContain('("sourceRawRowId")');
    for (const indexName of [
      "ptrs_dataset_reporting_entity_customer_idx",
      "ptrs_dataset_reporting_entity_ptrs_idx",
      "ptrs_dataset_reporting_entity_dataset_ux",
      "ptrs_dataset_reporting_entity_scope_ux",
      "ptrs_dataset_reporting_entity_abn_idx",
    ]) {
      expect(migration).toContain(`IF NOT EXISTS "${indexName}"`);
    }

    const abnEvidenceMigration = fs.readFileSync(
      path.join(
        __dirname,
        "../../../db/migrations/20260914_ptrs_dataset_reporting_entity_abn_evidence.sql",
      ),
      "utf8",
    );
    expect(abnEvidenceMigration).toContain(
      'DROP CONSTRAINT IF EXISTS "ptrs_dataset_reporting_entity_abn_ck"',
    );
    expect(abnEvidenceMigration).toContain('ALTER COLUMN "abn" TYPE text');
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
    expect(migration).toContain('"customerId", "ptrsId", "datasetId", "rowNo"');
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
