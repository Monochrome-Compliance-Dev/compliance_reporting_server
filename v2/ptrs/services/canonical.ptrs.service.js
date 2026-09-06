const { Op } = require("sequelize");
const { randomUUID } = require("crypto");
const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { logger } = require("@/helpers/logger");
const { buildStableInputHash } = require("@/v2/ptrs/services/ptrs.service");
const {
  composeMappedRowsForPtrs,
  prepareMappedRowsContext,
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

const CANONICAL_VERSION = "ptrs-canonical-v3";
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
  const conditions = (
    Array.isArray(joins?.conditions) ? joins.conditions : []
  ).filter((condition) => {
    const fromId = String(condition?.from?.datasetId || "");
    const toId = String(condition?.to?.datasetId || "");
    return reachableIds.has(fromId) && reachableIds.has(toId);
  });
  const scopedCustomFields = (
    Array.isArray(customFields) ? customFields : []
  ).filter((field) => reachableIds.has(String(field?.datasetId || "")));
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
  if (dataset.status !== "parsed" && dataset.sourceFormat !== "api") {
    const error = new Error("Selected transaction dataset has not been parsed");
    error.statusCode = 400;
    throw error;
  }
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
    order: [
      ["canonicalField", "ASC"],
      ["datasetId", "ASC"],
    ],
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
    datasetId: row.datasetId,
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
    preparedInput: freezeInput(
      JSON.parse(
        JSON.stringify({
          customerId,
          ptrsId,
          profileId,
          transactionDataset: dataset,
          datasets: relatedDatasets,
          supportConfig: { profileId, ...enrichment },
          fieldMapRows: mappingMaterial,
        }),
      ),
    ),
    inputSnapshot: {
      ...material,
      source,
      references,
      enrichment,
      mappings: mappingMaterial,
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

function freezeInput(value) {
  if (value && typeof value === "object") {
    Object.values(value).forEach(freezeInput);
    Object.freeze(value);
  }
  return value;
}

function sanitizeJson(value) {
  return JSON.parse(
    JSON.stringify(value, (_, item) =>
      typeof item === "string" ? item.replace(/\u0000/g, "") : item,
    ),
  );
}

async function rollbackCanonicalTransaction(transaction, meta) {
  if (!transaction || transaction.finished) return;
  try {
    await transaction.rollback();
  } catch (rollbackError) {
    logger.error("PTRS canonical rollback failed", {
      ...meta,
      rollbackError: rollbackError.message,
    });
  }
}

async function beginCanonicalTransaction(customerId) {
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    // SET LOCAL tenant context does not take a data snapshot. Set isolation
    // before the first read, for both configuration capture and the build.
    await db.sequelize.query(
      "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
      {
        transaction,
      },
    );
    return transaction;
  } catch (error) {
    await rollbackCanonicalTransaction(transaction, { customerId });
    throw error;
  }
}

async function assertCanonicalSourcesUnchanged(snapshot, transaction) {
  const { customerId, ptrsId, datasets } = snapshot.preparedInput;
  const current = await db.PtrsDataset.findAll({
    where: {
      customerId,
      ptrsId,
      id: { [Op.in]: datasets.map((item) => item.id) },
    },
    order: [["id", "ASC"]],
    raw: true,
    transaction,
  });
  const actual = [];
  for (const dataset of current) {
    actual.push(await datasetContentSnapshot(dataset, transaction));
  }
  const expected = [
    snapshot.inputSnapshot.source,
    ...snapshot.inputSnapshot.references,
  ].sort((a, b) => a.id.localeCompare(b.id));
  actual.sort((a, b) => a.id.localeCompare(b.id));
  if (buildStableInputHash(actual) !== buildStableInputHash(expected)) {
    const error = new Error(
      "Canonical source data changed before execution; retry with current inputs",
    );
    error.code = "CANONICAL_INPUT_CHANGED";
    error.statusCode = 409;
    throw error;
  }
}

async function recordCanonicalFailure(revision, error, meta) {
  let transaction;
  try {
    transaction = await beginTransactionWithCustomerContext(meta.customerId);
    const failed = await db.PtrsCanonicalRevision.findOne({
      where: {
        id: revision.id,
        customerId: meta.customerId,
        ptrsId: meta.ptrsId,
        datasetId: meta.datasetId,
        status: "building",
      },
      transaction,
      lock: "UPDATE",
    });
    // A commit acknowledgement can fail after successful publication. Never
    // overwrite succeeded output, including in that ambiguous failure case.
    if (failed)
      await failed.update(
        {
          status: "failed",
          completedAt: new Date(),
          failure: { message: error.message, code: error.code || null },
        },
        { transaction },
      );
    await transaction.commit();
  } catch (failureError) {
    await rollbackCanonicalTransaction(transaction, meta);
    logger.error(
      "Could not persist failed PTRS canonical revision; recovery required",
      {
        ...meta,
        canonicalRevisionId: revision.id,
        error: failureError.message,
        originalError: error.message,
      },
    );
  }
}

async function materializeCanonicalRevision({
  customerId,
  ptrsId,
  datasetId,
  profileId,
  actorId = null,
  requestId = null,
  compose = composeMappedRowsForPtrs,
  prepare = prepareMappedRowsContext,
}) {
  const started = process.hrtime.bigint();
  const elapsed = (since) => Number(process.hrtime.bigint() - since) / 1e6;
  const meta = {
    customerId,
    ptrsId,
    datasetId,
    primaryDatasetId: datasetId,
    profileId,
    requestId,
    operationId: randomUUID(),
  };
  const emit = (event, details = {}) =>
    logger.info("PTRS canonical lifecycle", {
      ...meta,
      event,
      ...details,
    });
  const memory = () => {
    const { rss, heapUsed } = process.memoryUsage();
    return { rssBytes: rss, heapUsedBytes: heapUsed };
  };
  const activeWhere = (materialSignature) => ({
    customerId,
    ptrsId,
    datasetId,
    materialSignature,
    status: { [Op.in]: ["building", "succeeded"] },
  });
  let setupTransaction;
  let buildTransaction;
  let revision;
  let snapshot;
  let ownsRevision = false;
  emit("start", memory());
  try {
    setupTransaction = await beginCanonicalTransaction(customerId);
    snapshot = await buildCanonicalInputSnapshot({
      customerId,
      ptrsId,
      datasetId,
      profileId,
      transaction: setupTransaction,
    });
    const existing = await db.PtrsCanonicalRevision.findOne({
      where: activeWhere(snapshot.materialSignature),
      raw: true,
      transaction: setupTransaction,
    });
    meta.materialSignature = snapshot.materialSignature;
    emit("input_prepared", { durationMs: elapsed(started) });
    if (existing) {
      await setupTransaction.commit();
      emit(existing.status === "succeeded" ? "reused" : "contended", {
        canonicalRevisionId: existing.id,
      });
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
    // The committed unique active-material insert is the ownership claim.
    // Only this request may enter the build. No age-based takeover exists.
    ownsRevision = true;
    await setupTransaction.commit();
    meta.canonicalRevisionId = revision.id;
    emit("ownership_acquired");
    buildTransaction = await beginCanonicalTransaction(customerId);
    revision = await db.PtrsCanonicalRevision.findOne({
      where: {
        id: revision.id,
        customerId,
        ptrsId,
        datasetId,
        status: "building",
      },
      transaction: buildTransaction,
      lock: "UPDATE",
    });
    if (!revision)
      throw new Error("Canonical execution claim is no longer building");
    const [pidRows] = await db.sequelize.query(
      "SELECT pg_backend_pid() AS pid",
      {
        transaction: buildTransaction,
      },
    );
    meta.backendPid = pidRows[0].pid;
    await assertCanonicalSourcesUnchanged(snapshot, buildTransaction);
    const preparationStarted = process.hrtime.bigint();
    const preparedContext = await prepare({
      customerId,
      ptrsId,
      datasetId,
      preparedInput: snapshot.preparedInput,
      transaction: buildTransaction,
      hrMsSince: elapsed,
      parseDateFlexible,
      trace: { write: emit },
    });
    emit("context_prepared", {
      durationMs: elapsed(preparationStarted),
      ...memory(),
    });
    let afterRowNo = null;
    let rowCount = 0;
    let batchNumber = 0;
    while (true) {
      const composeStarted = process.hrtime.bigint();
      const result = await compose({
        customerId,
        ptrsId,
        datasetId,
        limit: CANONICAL_BATCH_SIZE,
        afterRowNo,
        transaction: buildTransaction,
        preparedContext,
        hrMsSince: elapsed,
        parseDateFlexible,
      });
      const rows = Array.isArray(result?.rows) ? result.rows : [];
      if (!rows.length) break;
      const composeMs = elapsed(composeStarted);
      const persistenceStarted = process.hrtime.bigint();
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
      batchNumber += 1;
      const previousRowNo = afterRowNo;
      afterRowNo = Number(payload.at(-1)?.sourceRowNo);
      if (
        !Number.isFinite(afterRowNo) ||
        (previousRowNo != null && afterRowNo <= previousRowNo)
      ) {
        throw new Error(
          "Canonical source rows require deterministic source row numbers",
        );
      }
      emit("batch_complete", {
        batchNumber,
        rows: payload.length,
        firstRowNo: payload[0].sourceRowNo,
        lastRowNo: afterRowNo,
        cumulativeRows: rowCount,
        composeMs,
        persistenceMs: elapsed(persistenceStarted),
      });
      if (rows.length < CANONICAL_BATCH_SIZE) break;
    }
    await revision.update(
      { status: "succeeded", rowCount, completedAt: new Date(), failure: null },
      { transaction: buildTransaction },
    );
    await buildTransaction.commit();
    emit("complete", {
      rowCount,
      batchCount: batchNumber,
      durationMs: elapsed(started),
      ...memory(),
    });
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
    await rollbackCanonicalTransaction(buildTransaction, meta);
    await rollbackCanonicalTransaction(setupTransaction, meta);
    // A concurrent claimant may have committed since our repeatable-read
    // snapshot. Resolve the unique-key race in a fresh transaction.
    if (
      !ownsRevision &&
      error.original?.constraint ===
        "ptrs_canonical_revision_active_material_ux"
    ) {
      let lookupTransaction;
      try {
        lookupTransaction =
          await beginTransactionWithCustomerContext(customerId);
        const active = await db.PtrsCanonicalRevision.findOne({
          where: activeWhere(snapshot.materialSignature),
          raw: true,
          transaction: lookupTransaction,
        });
        await lookupTransaction.commit();
        if (active) {
          emit(active.status === "succeeded" ? "reused" : "contended", {
            canonicalRevisionId: active.id,
          });
          return { revision: active, reused: true };
        }
      } catch (lookupError) {
        await rollbackCanonicalTransaction(lookupTransaction, meta);
        logger.error("PTRS canonical claim lookup failed", {
          ...meta,
          error: lookupError.message,
        });
      }
    }
    if (ownsRevision && revision)
      await recordCanonicalFailure(revision, error, meta);
    emit("failed", { error: error.message, durationMs: elapsed(started) });
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
