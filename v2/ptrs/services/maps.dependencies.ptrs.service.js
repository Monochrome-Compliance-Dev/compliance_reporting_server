const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const {
  getSupportConfig,
  getFieldMap,
} = require("@/v2/ptrs/services/maps.config.ptrs.service");

async function loadComposeDependencies({
  customerId,
  ptrsId,
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
      fieldMapRows = await getFieldMap({
        customerId,
        ptrsId,
        profileId,
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

    const fromRole = String(from.role || "").toLowerCase();
    const toRole = String(to.role || "").toLowerCase();

    const fromCol = from.column;
    const toCol = to.column;

    if (!fromRole || !toRole || !fromCol || !toCol) continue;

    normalisedJoins.push({
      fromRole,
      fromColumn: fromCol,
      fromTransform: from.transform || null,
      toRole,
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

async function resolveMainDatasetForCompose({
  customerId,
  ptrsId,
  transaction,
  stageStart,
  stageEnd,
}) {
  let mainDatasetId = null;
  const sMainDataset = stageStart("resolve_main_dataset");
  try {
    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      attributes: ["id", "role", "createdAt"],
      raw: true,
      transaction,
    });

    const normRole = (r) =>
      String(r || "")
        .trim()
        .toLowerCase();
    const byRole = (role) =>
      (dsRows || []).find((d) => normRole(d.role) === role);

    const main = byRole("main");
    const anchor = byRole("anchor");

    if (main && main.id) mainDatasetId = main.id;
    else if (anchor && anchor.id) mainDatasetId = anchor.id;
    else if (Array.isArray(dsRows) && dsRows.length === 1)
      mainDatasetId = dsRows[0].id;

    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: resolved main dataset",
      safeMeta({
        customerId,
        ptrsId,
        mainDatasetId,
        datasetCount: Array.isArray(dsRows) ? dsRows.length : 0,
        roles: Array.isArray(dsRows) ? dsRows.map((d) => d.role) : [],
      }),
    );
  } catch (e) {
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: failed to resolve main dataset; falling back to unscoped import_raw",
      safeMeta({ customerId, ptrsId, error: e.message }),
    );
    mainDatasetId = null;
  }
  stageEnd(sMainDataset, { mainDatasetId });
  return mainDatasetId;
}

async function loadMainRowsForCompose({
  customerId,
  ptrsId,
  mainDatasetId,
  limit,
  offset,
  transaction,
  stageStart,
  stageEnd,
}) {
  const findOpts = {
    where: mainDatasetId
      ? { customerId, ptrsId, datasetId: mainDatasetId }
      : { customerId, ptrsId },
    order: [["rowNo", "ASC"]],
    attributes: ["rowNo", "data"],
    raw: true,
    transaction,
  };

  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    findOpts.limit = Math.min(numericLimit, 5000);
  }
  if (Number.isFinite(offset) && offset >= 0) {
    findOpts.offset = offset;
  }

  const sLoadMain = stageStart("load_main_rows");
  const mainRows = await db.PtrsImportRaw.findAll(findOpts);
  stageEnd(sLoadMain, {
    rowsLoaded: Array.isArray(mainRows) ? mainRows.length : 0,
  });

  if (!mainDatasetId) {
    try {
      const dsCount = await db.PtrsDataset.count({
        where: { customerId, ptrsId },
        transaction,
      });
      if (dsCount > 1) {
        slog.warn(
          "PTRS v2 composeMappedRowsForPtrs: mainDatasetId not resolved while multiple datasets exist; mapped rows may include supporting datasets",
          safeMeta({ customerId, ptrsId, datasetCount: dsCount }),
        );
      }
    } catch (_) {}
  }

  return mainRows;
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
  normaliseConfiguredJoins,
  normaliseConfiguredCustomFields,
  resolveMainDatasetForCompose,
  loadMainRowsForCompose,
  buildHeadersFromComposedRows,
};
