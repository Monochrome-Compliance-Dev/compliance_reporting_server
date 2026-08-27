const db = require("@/db/database");
const { Op } = require("sequelize");
const { logger } = require("@/helpers/logger");
const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const {
  getSupportConfig,
} = require("@/v2/ptrs/services/maps.config.ptrs.service");

function normaliseJoinRole(role) {
  return String(role || "")
    .trim()
    .toLowerCase();
}

async function loadComposeDependencies({
  customerId,
  ptrsId,
  datasetId,
  transaction,
  trace,
  stageStart,
  stageEnd,
}) {
  const sLoadMap = stageStart("load_support_config");
  const supportConfigRow = await getSupportConfig({
    customerId,
    ptrsId,
    transaction,
  });

  stageEnd(sLoadMap, {
    hasSupportConfig: !!supportConfigRow,
    profileId: supportConfigRow?.profileId || null,
    hasJoins: !!(supportConfigRow && supportConfigRow.joins),
    hasCustomFields: !!(supportConfigRow && supportConfigRow.customFields),
    hasRowRules: !!(supportConfigRow && supportConfigRow.rowRules),
  });

  const supportConfig = supportConfigRow || {};
  const profileId = supportConfig.profileId || null;

  const sFieldMap = stageStart("load_field_map");
  let fieldMapRows = [];
  try {
    if (profileId) {
      const joins = supportConfig?.joins;
      const parsedJoins =
        typeof joins === "string" ? JSON.parse(joins) : joins || {};
      const reachableDatasetIds = new Set([String(datasetId)]);
      let changed = true;
      while (changed) {
        changed = false;
        for (const condition of Array.isArray(parsedJoins?.conditions)
          ? parsedJoins.conditions
          : []) {
          const fromDatasetId = String(condition?.from?.datasetId || "");
          const toDatasetId = String(condition?.to?.datasetId || "");
          if (!fromDatasetId || !toDatasetId) continue;
          if (
            reachableDatasetIds.has(fromDatasetId) &&
            !reachableDatasetIds.has(toDatasetId)
          ) {
            reachableDatasetIds.add(toDatasetId);
            changed = true;
          }
          if (
            reachableDatasetIds.has(toDatasetId) &&
            !reachableDatasetIds.has(fromDatasetId)
          ) {
            reachableDatasetIds.add(fromDatasetId);
            changed = true;
          }
        }
      }
      fieldMapRows = await db.PtrsFieldMap.findAll({
        where: {
          customerId,
          ptrsId,
          profileId,
          datasetId: { [Op.in]: Array.from(reachableDatasetIds) },
        },
        order: [["canonicalField", "ASC"]],
        raw: true,
        transaction,
      });
    }
  } catch (e) {
    slog.error(
      "PTRS v2 composeMappedRowsForPtrs: failed to load field map for profiled run",
      safeMeta({ customerId, ptrsId, profileId, error: e.message }),
    );
    e.statusCode = e.statusCode || 500;
    throw e;
  }

  stageEnd(sFieldMap, {
    profileId,
    fieldMapCount: Array.isArray(fieldMapRows) ? fieldMapRows.length : 0,
  });

  if (
    profileId &&
    (!Array.isArray(fieldMapRows) || fieldMapRows.length === 0)
  ) {
    const e = new Error(
      "Mapped dataset build requires at least one canonical field mapping for the active profile.",
    );
    e.statusCode = 400;
    throw e;
  }

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: field map loaded",
      safeMeta({
        customerId,
        ptrsId,
        profileId,
        datasetId,
        fieldMapCount: Array.isArray(fieldMapRows) ? fieldMapRows.length : 0,
      }),
    );
  }

  return {
    supportConfigRow,
    supportConfig,
    profileId,
    fieldMapRows,
  };
}

function normaliseConfiguredJoins({
  supportConfig,
  customerId,
  ptrsId,
  trace,
}) {
  if (logger && logger.debug) {
    slog.debug(
      "PTRS v2 composeMappedRowsForPtrs: raw joins",
      safeMeta({
        customerId,
        ptrsId,
        hasJoins: !!supportConfig.joins,
        joinsType: supportConfig.joins ? typeof supportConfig.joins : null,
      }),
    );
  }

  let joins = supportConfig.joins;
  if (typeof joins === "string") {
    try {
      joins = JSON.parse(joins);
    } catch {
      joins = null;
    }
  }

  let joinsArray = [];
  if (Array.isArray(joins)) {
    joinsArray = joins;
  } else if (joins && Array.isArray(joins.conditions)) {
    joinsArray = joins.conditions;
  } else {
    joinsArray = [];
  }

  const normalisedJoins = [];
  for (const j of joinsArray) {
    if (!j || typeof j !== "object") continue;

    const from = j.from || {};
    const to = j.to || {};

    const fromRole = normaliseJoinRole(from.role);
    const toRole = normaliseJoinRole(to.role);

    const fromCol = from.column;
    const toCol = to.column;

    const fromDatasetId = String(from.datasetId || "").trim();
    const toDatasetId = String(to.datasetId || "").trim();

    if (
      !fromRole ||
      !toRole ||
      !fromDatasetId ||
      !toDatasetId ||
      !fromCol ||
      !toCol
    ) {
      const error = new Error(
        "Every join endpoint requires datasetId, role and column",
      );
      error.statusCode = 400;
      throw error;
    }

    normalisedJoins.push({
      fromRole,
      fromDatasetId,
      fromColumn: fromCol,
      fromTransform: from.transform || null,
      toRole,
      toDatasetId,
      toColumn: toCol,
      toTransform: to.transform || null,
    });
  }

  trace?.write("compose_joins_normalised", {
    joinsRawType: joins == null ? null : typeof joins,
    joinsCount: normalisedJoins.length,
  });

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: normalised joins",
      safeMeta({ customerId, ptrsId, joinsCount: normalisedJoins.length }),
    );
  }

  return {
    joins,
    normalisedJoins,
  };
}

function normaliseConfiguredCustomFields({
  supportConfig,
  customerId,
  ptrsId,
  trace,
}) {
  let customFields = supportConfig.customFields;
  if (typeof customFields === "string") {
    try {
      customFields = JSON.parse(customFields);
    } catch {
      customFields = null;
    }
  }
  if (!Array.isArray(customFields)) {
    customFields = [];
  }

  trace?.write("compose_custom_fields_normalised", {
    customFieldsRawType:
      supportConfig.customFields == null
        ? null
        : typeof supportConfig.customFields,
    customFieldsCount: Array.isArray(customFields) ? customFields.length : 0,
  });

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: custom fields normalised",
      safeMeta({
        customerId,
        ptrsId,
        customFieldsCount: Array.isArray(customFields)
          ? customFields.length
          : 0,
        customFieldsType: customFields ? typeof customFields : null,
      }),
    );
  }

  return customFields;
}

async function resolveTransactionDatasetForCompose({
  customerId,
  ptrsId,
  datasetId,
  transaction,
  stageStart,
  stageEnd,
}) {
  if (!datasetId) {
    const error = new Error("datasetId is required for mapped dataset compose");
    error.statusCode = 400;
    throw error;
  }

  const stage = stageStart("resolve_transaction_dataset");
  const dataset = await db.PtrsDataset.findOne({
    where: { id: datasetId, customerId, ptrsId, purpose: "transaction" },
    attributes: [
      "id",
      "purpose",
      "sourceFormat",
      "adapterType",
      "status",
    ],
    raw: true,
    transaction,
  });

  if (!dataset) {
    const error = new Error(
      "Selected dataset is not a transaction dataset for this PTRS",
    );
    error.statusCode = 400;
    throw error;
  }
  if (dataset.status !== "parsed" && dataset.sourceFormat !== "api") {
    const error = new Error("Selected transaction dataset has not been parsed");
    error.statusCode = 400;
    throw error;
  }

  stageEnd(stage, { datasetId: dataset.id });
  return dataset;
}

async function loadTransactionRowsForCompose({
  customerId,
  ptrsId,
  datasetId,
  limit,
  offset,
  afterRowNo = null,
  transaction,
  stageStart,
  stageEnd,
}) {
  if (!datasetId) throw new Error("datasetId is required");
  const findOpts = {
    where: {
      customerId,
      ptrsId,
      datasetId,
      ...(afterRowNo == null
        ? {}
        : { rowNo: { [Op.gt]: Number(afterRowNo) } }),
    },
    order: [["rowNo", "ASC"]],
    attributes: ["id", "rowNo", "data"],
    raw: true,
    transaction,
  };

  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    findOpts.limit = Math.min(numericLimit, 5000);
  }
  if (afterRowNo == null && Number.isFinite(offset) && offset >= 0) {
    findOpts.offset = offset;
  }

  const stage = stageStart("load_transaction_rows");
  const rows = await db.PtrsImportRaw.findAll(findOpts);
  stageEnd(stage, {
    datasetId,
    rowsLoaded: Array.isArray(rows) ? rows.length : 0,
  });
  return rows;
}

function buildHeadersFromComposedRows(rows) {
  const headerSet = new Set();
  const list = Array.isArray(rows) ? rows : [];
  for (let i = 0; i < list.length && i < 200; ++i) {
    const row = list[i];
    for (const k of Object.keys(row || {})) {
      if (headerSet.size < 2000) headerSet.add(k);
    }
    if (headerSet.size >= 2000) break;
  }
  return Array.from(headerSet);
}

module.exports = {
  loadComposeDependencies,
  normaliseJoinRole,
  normaliseConfiguredJoins,
  normaliseConfiguredCustomFields,
  resolveTransactionDatasetForCompose,
  loadTransactionRowsForCompose,
  buildHeadersFromComposedRows,
};
