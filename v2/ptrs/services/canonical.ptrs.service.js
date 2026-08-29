const { Op } = require("sequelize");
const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { logger } = require("@/helpers/logger");
const { buildStableInputHash } = require("@/v2/ptrs/services/ptrs.service");
const {
  composeMappedRowsForPtrs,
} = require("@/v2/ptrs/services/maps.compose.ptrs.service");
const {
  buildStageColumnProjection,
} = require("@/v2/ptrs/services/stage.payment-time.ptrs.service");
const {
  getCanonicalAdapterContract,
  validateAdapterMappings,
  validateCanonicalRowForAdapter,
} = require("@/v2/ptrs/services/canonical.adapters.ptrs.service");
const {
  SAP_INVOICE_DATE_POLICY_VERSION,
} = require("@/v2/ptrs/services/canonical.date-policy.ptrs.service");

const CANONICAL_VERSION = "ptrs-canonical-v2";
const CANONICAL_BATCH_SIZE = 2000;

function parseDateFlexible(value) {
  if (value == null) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
  const text = String(value).trim();
  if (!text) return null;
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text);
  if (match) {
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = Number(match[3]);
    const date = new Date(Date.UTC(year, month - 1, day));
    return date.getUTCFullYear() === year &&
      date.getUTCMonth() === month - 1 &&
      date.getUTCDate() === day
      ? date
      : null;
  }
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? null : date;
}

function parseJson(value, fallback) {
  if (value == null) return fallback;
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function resolveCanonicalAdapter(dataset) {
  const adapterType = String(dataset?.adapterType || "").trim();
  const contract = getCanonicalAdapterContract(adapterType);
  if (contract) {
    const adapterVersion = String(
      dataset?.adapterVersion || contract.defaultVersion,
    ).trim();
    if (!contract.supportedVersions.includes(adapterVersion)) {
      const error = new Error(
        `Unsupported PTRS canonical adapter version for dataset ${dataset?.id || "(unknown)"}: ${adapterType}@${adapterVersion}`,
      );
      error.statusCode = 400;
      error.code = "UNSUPPORTED_CANONICAL_ADAPTER_VERSION";
      throw error;
    }
    return {
      adapterType,
      adapterVersion,
      semanticKind: contract.semanticKind,
      contract,
    };
  }
  const error = new Error(
    `Unsupported PTRS canonical adapter for dataset ${dataset?.id || "(unknown)"}: ${adapterType || "(missing)"}`,
  );
  error.statusCode = 400;
  error.code = "UNSUPPORTED_CANONICAL_ADAPTER";
  throw error;
}

function getReachableDatasetIds(
  joins,
  transactionDatasetId,
  blockedTransactionIds = new Set(),
) {
  const conditions = Array.isArray(joins?.conditions) ? joins.conditions : [];
  const transactionId = String(transactionDatasetId);
  const reachable = new Set([transactionId]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const condition of conditions) {
      const fromId = String(condition?.from?.datasetId || "");
      const toId = String(condition?.to?.datasetId || "");
      if (!fromId || !toId) continue;
      if (
        reachable.has(fromId) &&
        !reachable.has(toId) &&
        !blockedTransactionIds.has(toId)
      ) {
        reachable.add(toId);
        changed = true;
      }
      if (
        reachable.has(toId) &&
        !reachable.has(fromId) &&
        !blockedTransactionIds.has(fromId)
      ) {
        reachable.add(fromId);
        changed = true;
      }
    }
  }
  return reachable;
}

function filterMaterialEnrichment({ joins, customFields, reachableIds }) {
  const conditions = (Array.isArray(joins?.conditions) ? joins.conditions : [])
    .filter((condition) => {
      const fromId = String(condition?.from?.datasetId || "");
      const toId = String(condition?.to?.datasetId || "");
      return reachableIds.has(fromId) && reachableIds.has(toId);
    })
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  const scopedCustomFields = (Array.isArray(customFields) ? customFields : [])
    .filter((field) => reachableIds.has(String(field?.datasetId || "")))
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return { joins: { conditions }, customFields: scopedCustomFields };
}

async function datasetContentSnapshot(dataset, transaction) {
  const where = {
    customerId: dataset.customerId,
    ptrsId: dataset.ptrsId,
    datasetId: dataset.id,
  };
  const rawCount = await db.PtrsImportRaw.count({ where, transaction });
  const rawMaxUpdatedAt = await db.PtrsImportRaw.max("updatedAt", {
    where,
    transaction,
  });
  return {
    id: dataset.id,
    purpose: dataset.purpose,
    referenceKind: dataset.referenceKind || null,
    sourceFormat: dataset.sourceFormat,
    adapterType: dataset.adapterType || null,
    adapterVersion: dataset.adapterVersion || null,
    sourceGroupScope: dataset.sourceGroupScope || null,
    status: dataset.status,
    rowsCount: Number(dataset.rowsCount) || Number(rawCount) || 0,
    updatedAt: dataset.updatedAt || null,
    rawCount: Number(rawCount) || 0,
    rawMaxUpdatedAt: rawMaxUpdatedAt || null,
  };
}

async function buildCanonicalInputSnapshot({
  customerId,
  ptrsId,
  datasetId,
  profileId,
  transaction,
}) {
  if (!customerId || !ptrsId || !datasetId || !profileId) {
    throw new Error("customerId, ptrsId, datasetId and profileId are required");
  }
  const dataset = await db.PtrsDataset.findOne({
    where: { id: datasetId, customerId, ptrsId, purpose: "transaction" },
    raw: true,
    transaction,
  });
  if (!dataset) {
    const error = new Error("Transaction dataset not found");
    error.statusCode = 404;
    throw error;
  }
  const adapter = resolveCanonicalAdapter(dataset);
  const supportConfig = await db.PtrsColumnMap.findOne({
    where: { customerId, ptrsId },
    attributes: ["joins", "customFields"],
    raw: true,
    transaction,
  });
  const transactionDatasets = await db.PtrsDataset.findAll({
    where: { customerId, ptrsId, purpose: "transaction" },
    attributes: ["id"],
    raw: true,
    transaction,
  });
  const joins = parseJson(supportConfig?.joins, { conditions: [] });
  const customFields = parseJson(supportConfig?.customFields, []);
  const blockedTransactionIds = new Set(
    transactionDatasets
      .map((item) => String(item.id))
      .filter((id) => id !== String(datasetId)),
  );
  const reachableIds = getReachableDatasetIds(
    joins,
    datasetId,
    blockedTransactionIds,
  );
  const fieldMap = await db.PtrsFieldMap.findAll({
    where: {
      customerId,
      ptrsId,
      profileId,
      datasetId: { [Op.in]: Array.from(reachableIds) },
    },
    attributes: [
      "datasetId",
      "canonicalField",
      "sourceRole",
      "sourceColumn",
      "transformType",
      "transformConfig",
      "meta",
    ],
    order: [["canonicalField", "ASC"]],
    raw: true,
    transaction,
  });
  if (!fieldMap.length) {
    const error = new Error(
      `Transaction dataset ${datasetId} has no canonical field mappings`,
    );
    error.statusCode = 400;
    throw error;
  }
  validateAdapterMappings({
    contract: adapter.contract,
    fieldMap,
    datasetId,
  });
  const enrichment = filterMaterialEnrichment({
    joins,
    customFields,
    reachableIds,
  });
  const relatedDatasets = await db.PtrsDataset.findAll({
    where: {
      customerId,
      ptrsId,
      id: { [Op.in]: Array.from(reachableIds) },
    },
    order: [["id", "ASC"]],
    raw: true,
    transaction,
  });
  if (relatedDatasets.length !== reachableIds.size) {
    const error = new Error("Canonical joins reference a missing dataset");
    error.statusCode = 400;
    throw error;
  }
  const contentSnapshots = [];
  for (const related of relatedDatasets) {
    contentSnapshots.push(await datasetContentSnapshot(related, transaction));
  }
  const source = contentSnapshots.find((item) => item.id === datasetId);
  const references = contentSnapshots.filter((item) => item.id !== datasetId);
  const mappingMaterial = fieldMap.map((row) => ({
    canonicalField: row.canonicalField,
    sourceRole: row.sourceRole,
    sourceColumn: row.sourceColumn,
    transformType: row.transformType || null,
    transformConfig: row.transformConfig || null,
    meta: row.meta || null,
  }));
  const sourceSignature = buildStableInputHash(source);
  const mappingSignature = buildStableInputHash(mappingMaterial);
  const enrichmentSignature = buildStableInputHash({ enrichment, references });
  const material = {
    canonicalVersion: CANONICAL_VERSION,
    ...(adapter.adapterType === "sap_accounting_event"
      ? { datePolicyVersion: SAP_INVOICE_DATE_POLICY_VERSION }
      : {}),
    profileId,
    sourceSignature,
    mappingSignature,
    enrichmentSignature,
    adapter,
    sourceGroupScope: dataset.sourceGroupScope || null,
  };
  return {
    dataset,
    adapter,
    sourceSignature,
    mappingSignature,
    enrichmentSignature,
    materialSignature: buildStableInputHash(material),
    inputSnapshot: {
      ...material,
      source,
      references,
      enrichment,
      adapterContract: {
        requiredFields: adapter.contract.requiredFields,
        requiredAnyGroups: adapter.contract.requiredAnyGroups,
        optionalFields: adapter.contract.optionalFields || [],
        canonicalProjection: adapter.contract.canonicalProjection,
        sourceGroupSemantics: adapter.contract.sourceGroupSemantics,
        paymentAmountSemantic: adapter.contract.paymentAmountSemantic || null,
      },
    },
  };
}

function sanitizeJson(value) {
  return JSON.parse(
    JSON.stringify(value, (_, item) =>
      typeof item === "string" ? item.replace(/\u0000/g, "") : item,
    ),
  );
}

async function materializeCanonicalRevision({
  customerId,
  ptrsId,
  datasetId,
  profileId,
  actorId = null,
  compose = composeMappedRowsForPtrs,
}) {
  let setupTransaction = await beginTransactionWithCustomerContext(customerId);
  let revision;
  let snapshot;
  try {
    snapshot = await buildCanonicalInputSnapshot({
      customerId,
      ptrsId,
      datasetId,
      profileId,
      transaction: setupTransaction,
    });
    const existing = await db.PtrsCanonicalRevision.findOne({
      where: {
        customerId,
        ptrsId,
        datasetId,
        materialSignature: snapshot.materialSignature,
        status: "succeeded",
      },
      raw: true,
      transaction: setupTransaction,
    });
    if (existing) {
      await setupTransaction.commit();
      return { revision: existing, reused: true };
    }
    revision = await db.PtrsCanonicalRevision.create(
      {
        customerId,
        ptrsId,
        datasetId,
        adapterType: snapshot.adapter.adapterType,
        adapterVersion: snapshot.adapter.adapterVersion,
        sourceGroupScope: snapshot.dataset.sourceGroupScope || null,
        canonicalVersion: CANONICAL_VERSION,
        semanticKind: snapshot.adapter.semanticKind,
        sourceSignature: snapshot.sourceSignature,
        mappingSignature: snapshot.mappingSignature,
        enrichmentSignature: snapshot.enrichmentSignature,
        materialSignature: snapshot.materialSignature,
        inputSnapshot: snapshot.inputSnapshot,
        status: "building",
        rowCount: null,
        createdBy: actorId,
      },
      { transaction: setupTransaction },
    );
    await setupTransaction.commit();
  } catch (error) {
    if (!setupTransaction.finished) await setupTransaction.rollback();
    throw error;
  }

  const buildTransaction =
    await beginTransactionWithCustomerContext(customerId);
  try {
    let afterRowNo = null;
    let rowCount = 0;
    while (true) {
      const result = await compose({
        customerId,
        ptrsId,
        datasetId,
        limit: CANONICAL_BATCH_SIZE,
        afterRowNo,
        transaction: buildTransaction,
        hrMsSince: () => 0,
        parseDateFlexible,
      });
      const rows = Array.isArray(result?.rows) ? result.rows : [];
      if (!rows.length) break;
      const payload = rows.map((row) => {
        const data = sanitizeJson(row);
        const meta = data?._ptrsMeta || {};
        const sourceRowNo = Number(meta.sourceRowNo);
        if (!Number.isFinite(sourceRowNo)) {
          throw new Error(
            "Canonical source row is missing its source row number",
          );
        }
        validateCanonicalRowForAdapter({
          contract: snapshot.adapter.contract,
          row: data,
          datasetId,
          sourceRowNo,
        });
        return {
          customerId,
          ptrsId,
          canonicalRevisionId: revision.id,
          datasetId,
          sourceRawRowId: meta.sourceRawRowId || null,
          sourceRowNo,
          sourceGroupScope: snapshot.dataset.sourceGroupScope || null,
          adapterType: snapshot.adapter.adapterType,
          adapterVersion: snapshot.adapter.adapterVersion,
          semanticKind: snapshot.adapter.semanticKind,
          ...buildStageColumnProjection(data, db.PtrsCanonicalSourceRow),
          data,
          provenance: {
            sourceDatasetId: datasetId,
            sourceRawRowId: meta.sourceRawRowId || null,
            sourceRowNo,
            canonicalSources: meta.canonicalSources || {},
            joinedReferences: meta.joinedReferences || {},
            materialSignature: snapshot.materialSignature,
          },
        };
      });
      await db.PtrsCanonicalSourceRow.bulkCreate(payload, {
        validate: true,
        transaction: buildTransaction,
      });
      rowCount += payload.length;
      afterRowNo = Number(payload.at(-1)?.sourceRowNo);
      if (!Number.isFinite(afterRowNo)) {
        throw new Error(
          "Canonical source rows require deterministic source row numbers",
        );
      }
      if (rows.length < CANONICAL_BATCH_SIZE) break;
    }
    await revision.update(
      { status: "succeeded", rowCount, completedAt: new Date(), failure: null },
      { transaction: buildTransaction },
    );
    await buildTransaction.commit();
    logger.info("PTRS canonical revision materialised", {
      action: "PtrsCanonicalRevisionMaterialised",
      customerId,
      ptrsId,
      datasetId,
      canonicalRevisionId: revision.id,
      rowCount,
      materialSignature: snapshot.materialSignature,
    });
    return {
      revision: revision.get ? revision.get({ plain: true }) : revision,
      reused: false,
    };
  } catch (error) {
    if (!buildTransaction.finished) await buildTransaction.rollback();
    const failureTransaction =
      await beginTransactionWithCustomerContext(customerId);
    try {
      const failed = await db.PtrsCanonicalRevision.findOne({
        where: { id: revision.id, customerId, ptrsId, datasetId },
        transaction: failureTransaction,
      });
      if (failed) {
        await failed.update(
          {
            status: "failed",
            completedAt: new Date(),
            failure: { message: error.message, code: error.code || null },
          },
          { transaction: failureTransaction },
        );
      }
      await failureTransaction.commit();
    } catch (failureError) {
      if (!failureTransaction.finished) await failureTransaction.rollback();
      logger.error("Could not persist failed PTRS canonical revision", {
        customerId,
        ptrsId,
        datasetId,
        canonicalRevisionId: revision.id,
        error: failureError.message,
      });
    }
    throw error;
  }
}

async function resolveCurrentCanonicalRevision({
  customerId,
  ptrsId,
  datasetId,
  profileId,
  transaction,
}) {
  const snapshot = await buildCanonicalInputSnapshot({
    customerId,
    ptrsId,
    datasetId,
    profileId,
    transaction,
  });
  const revision = await db.PtrsCanonicalRevision.findOne({
    where: {
      customerId,
      ptrsId,
      datasetId,
      materialSignature: snapshot.materialSignature,
      status: "succeeded",
    },
    order: [["createdAt", "DESC"]],
    raw: true,
    transaction,
  });
  return { revision, snapshot };
}

async function resolveCurrentCanonicalRevisions({
  customerId,
  ptrsId,
  profileId,
  transaction,
}) {
  const datasets = await db.PtrsDataset.findAll({
    where: { customerId, ptrsId, purpose: "transaction" },
    order: [
      ["createdAt", "ASC"],
      ["id", "ASC"],
    ],
    raw: true,
    transaction,
  });
  if (!datasets.length) {
    const error = new Error("No transaction datasets are selected for Stage");
    error.statusCode = 400;
    throw error;
  }
  const selected = [];
  const missing = [];
  for (
    let datasetOrder = 0;
    datasetOrder < datasets.length;
    datasetOrder += 1
  ) {
    const dataset = datasets[datasetOrder];
    const current = await resolveCurrentCanonicalRevision({
      customerId,
      ptrsId,
      datasetId: dataset.id,
      profileId,
      transaction,
    });
    if (!current.revision) {
      missing.push({
        datasetId: dataset.id,
        fileName: dataset.fileName || null,
        expectedMaterialSignature: current.snapshot.materialSignature,
      });
      continue;
    }
    selected.push({ dataset, datasetOrder, revision: current.revision });
  }
  if (missing.length) {
    const error = new Error(
      `Canonical rebuild required for transaction dataset(s): ${missing.map((item) => item.fileName || item.datasetId).join(", ")}`,
    );
    error.statusCode = 409;
    error.code = "CANONICAL_REVISION_REQUIRED";
    error.details = { missing };
    throw error;
  }
  return selected;
}

async function listCanonicalRevisionStatus({ customerId, ptrsId, profileId }) {
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    const datasets = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId, purpose: "transaction" },
      order: [
        ["createdAt", "ASC"],
        ["id", "ASC"],
      ],
      raw: true,
      transaction,
    });
    const statuses = [];
    for (const dataset of datasets) {
      let current = { revision: null, snapshot: null };
      let error = null;
      try {
        current = await resolveCurrentCanonicalRevision({
          customerId,
          ptrsId,
          datasetId: dataset.id,
          profileId,
          transaction,
        });
      } catch (statusError) {
        error = {
          code: statusError.code || null,
          message: statusError.message,
        };
      }
      const latest = await db.PtrsCanonicalRevision.findOne({
        where: { customerId, ptrsId, datasetId: dataset.id },
        order: [
          ["createdAt", "DESC"],
          ["id", "DESC"],
        ],
        raw: true,
        transaction,
      });
      statuses.push({
        datasetId: dataset.id,
        fileName: dataset.fileName || null,
        current: !!current.revision,
        expectedMaterialSignature: current.snapshot?.materialSignature || null,
        revision: current.revision || null,
        latest: latest || null,
        error,
      });
    }
    await transaction.commit();
    return statuses;
  } catch (error) {
    if (!transaction.finished) await transaction.rollback();
    throw error;
  }
}

async function loadCanonicalRevisionRows({
  customerId,
  ptrsId,
  revisionId,
  limit = 2000,
  afterSourceRowNo = null,
  transaction,
}) {
  const rows = await db.PtrsCanonicalSourceRow.findAll({
    where: {
      customerId,
      ptrsId,
      canonicalRevisionId: revisionId,
      ...(afterSourceRowNo == null
        ? {}
        : { sourceRowNo: { [Op.gt]: Number(afterSourceRowNo) } }),
    },
    order: [
      ["sourceRowNo", "ASC"],
      ["id", "ASC"],
    ],
    limit: Math.min(Math.max(Number(limit) || 1, 1), 5000),
    raw: true,
    transaction,
  });
  return rows.map((row) => ({
    ...row.data,
    row_no: row.sourceRowNo,
    _canonicalProvenance: {
      canonicalRevisionId: row.canonicalRevisionId,
      canonicalSourceRowId: row.id,
      datasetId: row.datasetId,
      sourceRawRowId: row.sourceRawRowId || null,
      sourceRowNo: row.sourceRowNo,
      adapterType: row.adapterType,
      adapterVersion: row.adapterVersion || null,
      sourceGroupScope: row.sourceGroupScope || null,
      semanticKind: row.semanticKind,
      lineage: row.provenance || {},
    },
  }));
}

module.exports = {
  CANONICAL_VERSION,
  CANONICAL_BATCH_SIZE,
  resolveCanonicalAdapter,
  getReachableDatasetIds,
  filterMaterialEnrichment,
  buildCanonicalInputSnapshot,
  materializeCanonicalRevision,
  resolveCurrentCanonicalRevision,
  resolveCurrentCanonicalRevisions,
  listCanonicalRevisionStatus,
  loadCanonicalRevisionRows,
};
