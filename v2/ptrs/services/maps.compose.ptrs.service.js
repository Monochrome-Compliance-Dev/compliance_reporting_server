const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  safeMeta,
  slog,
  normalizeJoinKeyValue,
  toSnake,
} = require("@/v2/ptrs/services/ptrs.service");
const { pickFromRowLoose } = require("@/v2/ptrs/services/data.ptrs.service");
const {
  logComposeJoinProbeOnce,
} = require("@/v2/ptrs/services/maps.joins.ptrs.service");
const {
  loadComposeDependencies,
  normaliseConfiguredJoins,
  normaliseConfiguredCustomFields,
  resolveTransactionDatasetForCompose,
  loadTransactionRowsForCompose,
  buildHeadersFromComposedRows,
} = require("@/v2/ptrs/services/maps.dependencies.ptrs.service");
const {
  applyCanonicalInvoiceDatePolicy,
} = require("@/v2/ptrs/services/canonical.date-policy.ptrs.service");

function orderJoinsForTransactionDataset(joins, transactionDatasetId) {
  const list = Array.isArray(joins) ? joins.slice() : [];
  const available = new Set([String(transactionDatasetId || "")]);
  let remaining = list;
  const ordered = [];

  let guard = 0;
  while (remaining.length) {
    guard += 1;
    if (guard > list.length + 10) break;

    const passPicked = [];
    const passLeft = [];

    for (const join of remaining) {
      const fromDatasetId = String(join.fromDatasetId || "");
      const toDatasetId = String(join.toDatasetId || "");

      if (!fromDatasetId || !toDatasetId) {
        passPicked.push(join);
        continue;
      }

      if (available.has(fromDatasetId) || available.has(toDatasetId)) {
        passPicked.push(join);
      } else {
        passLeft.push(join);
      }
    }

    if (!passPicked.length) {
      // The stored config can contain independent join graphs for other
      // transaction datasets. Composition is scoped to the graph reachable
      // from the explicitly selected transaction dataset only.
      break;
    }

    for (const join of passPicked) {
      ordered.push(join);
      if (join.fromDatasetId) available.add(String(join.fromDatasetId));
      if (join.toDatasetId) available.add(String(join.toDatasetId));
    }

    remaining = passLeft;
  }

  return ordered;
}

function applyCustomFields({
  row,
  rawRow,
  customFields,
  rowDatasetId,
}) {
  const out = { ...(row || {}) };
  const source = rawRow && typeof rawRow === "object" ? rawRow : {};
  const currentDatasetId = String(rowDatasetId || "").trim();

  const nsKey = (datasetId, col) => `${String(datasetId)}__${String(col)}`;

  for (const cf of Array.isArray(customFields) ? customFields : []) {
    if (!cf || typeof cf !== "object") continue;

    const customFieldDatasetId = String(cf.datasetId || "").trim();
    if (
      !currentDatasetId ||
      !customFieldDatasetId ||
      customFieldDatasetId !== currentDatasetId
    ) {
      continue;
    }

    const target = cf.key || cf.field || cf.target || cf.name;
    if (!target) continue;

    let value = null;

    const resolveFieldValue = (sourceDatasetId, sourceColumn) => {
      if (!sourceColumn) return null;
      const datasetId = String(sourceDatasetId || cf.datasetId || "").trim();
      if (!datasetId || datasetId === currentDatasetId) {
        return (
          pickFromRowLoose(out, sourceColumn) ??
          pickFromRowLoose(source, sourceColumn)
        );
      }

      return (
        pickFromRowLoose(out, nsKey(datasetId, sourceColumn)) ??
        pickFromRowLoose(source, nsKey(datasetId, sourceColumn))
      );
    };

    if (Object.prototype.hasOwnProperty.call(cf, "value")) {
      value = cf.value;
    } else if (
      String(cf.type || "")
        .trim()
        .toLowerCase() === "concat"
    ) {
      const segments = Array.isArray(cf.segments) ? cf.segments : [];
      const parts = [];

      for (const segment of segments) {
        if (!segment || typeof segment !== "object") continue;

        const kind = String(segment.kind || "field")
          .trim()
          .toLowerCase();

        if (kind === "literal") {
          parts.push(segment.value == null ? "" : String(segment.value));
          continue;
        }

        const fieldName =
          segment.name || segment.field || segment.column || null;
        const fieldDatasetId = segment.datasetId || cf.datasetId || null;
        const fieldValue = resolveFieldValue(fieldDatasetId, fieldName);

        parts.push(fieldValue == null ? "" : String(fieldValue));
      }

      value = parts.join("");
    } else {
      const sourceColumn =
        cf.sourceColumn || cf.column || cf.sourceHeader || cf.header || null;

      if (!sourceColumn) continue;

      value = resolveFieldValue(cf.datasetId || null, sourceColumn);
    }

    out[target] = value == null ? null : value;
  }

  return out;
}

function applyCanonicalProjectionForCompose({
  out,
  srcRow,
  fieldMapRows,
  resolveCanonicalValue,
  applyTransform,
  setCanonicalSourceMeta,
  counters,
}) {
  const nextOut = { ...(out || {}) };
  const canonicalOut = {};

  for (const fm of fieldMapRows || []) {
    if (!fm || typeof fm !== "object") continue;

    const canonicalKey = toSnake(fm.canonicalField);
    if (!canonicalKey) continue;

    const rawValue = resolveCanonicalValue({
      sourceRole: fm.sourceRole,
      sourceDatasetId: fm.datasetId || null,
      sourceColumn: fm.sourceColumn,
      srcRow,
      outRow: nextOut,
    });

    const transformed = applyTransform({
      value: rawValue,
      transformType: fm.transformType,
      transformConfig: fm.transformConfig,
    });

    const hasCanonicalValue =
      transformed != null && String(transformed).trim() !== "";

    if (hasCanonicalValue) {
      canonicalOut[canonicalKey] = transformed;

      const withMeta = setCanonicalSourceMeta({
        outRow: nextOut,
        canonicalField: canonicalKey,
        sourceRole: fm.sourceRole || null,
        sourceDatasetId: fm.datasetId || null,
        sourceColumn: fm.sourceColumn || null,
        transformType: fm.transformType || null,
      });

      Object.assign(nextOut, withMeta);
      counters.canonicalSourceMetaApplied += 1;
    }
  }

  return {
    ...nextOut,
    ...canonicalOut,
  };
}

function attachCanonicalSourceLineage({ row, rawRow, transactionDatasetId }) {
  const out = { ...(row || {}) };
  const existing =
    out._ptrsMeta && typeof out._ptrsMeta === "object"
      ? { ...out._ptrsMeta }
      : {};
  out._ptrsMeta = {
    ...existing,
    sourceDatasetId: String(transactionDatasetId),
    sourceRawRowId: rawRow?.id || null,
    sourceRowNo: Number(rawRow?.rowNo),
  };
  return out;
}

async function composeSingleMappedRow({
  rawRow,
  orderedJoins,
  customFields,
  fieldMapRows,
  preparedJoinIndexes,
  counters,
  customerId,
  ptrsId,
  logger,
  loggedJoinProbeRef,
  transactionDatasetId,
  adapterType,
  getJoinLhsValue,
  mergeRoleRowNamespaced,
  joinIndexKey,
  resolveCanonicalValue,
  applyTransform,
  setCanonicalSourceMeta,
  normalizeJoinKeyValue,
  logComposeJoinProbeOnce,
}) {
  let base = rawRow?.data || {};
  if (typeof base === "string") {
    try {
      const parsed = JSON.parse(base);
      if (parsed && typeof parsed === "object") {
        base = parsed;
      }
    } catch {
      base = {};
    }
  }
  if (!base || typeof base !== "object") {
    base = {};
  }
  const baseWithCustomFields =
    Array.isArray(customFields) && customFields.length
      ? applyCustomFields({
          row: base,
          rawRow: base,
          customFields,
          rowDatasetId: transactionDatasetId,
        })
      : { ...(base || {}) };

  let srcRow = baseWithCustomFields;

  if (orderedJoins.length) {
    let workingRow = srcRow;
    const presentDatasetIds = new Set([String(transactionDatasetId)]);

    for (const j of orderedJoins) {
      counters.joinAttempts += 1;
      const fromRole = String(j.fromRole || "").toLowerCase();
      const toRole = String(j.toRole || "").toLowerCase();
      const fromDatasetId = String(j.fromDatasetId || "");
      const toDatasetId = String(j.toDatasetId || "");

      const fromCol = j.fromColumn;
      const toCol = j.toColumn;

      const fromTransform = j.fromTransform || null;
      const toTransform = j.toTransform || null;

      if (
        !fromRole ||
        !toRole ||
        !fromDatasetId ||
        !toDatasetId ||
        !fromCol ||
        !toCol
      ) {
        continue;
      }

      const fromPresent = presentDatasetIds.has(fromDatasetId);
      const toPresent = presentDatasetIds.has(toDatasetId);

      let sourceRole = null;
      let sourceDatasetId = null;
      let sourceCol = null;
      let sourceTransform = null;
      let lookupRole = null;
      let lookupDatasetId = null;
      let lookupCol = null;
      let lookupTransform = null;
      let mergeRole = null;
      let mergeDatasetId = null;

      if (fromPresent && !toPresent) {
        sourceRole = fromRole;
        sourceDatasetId = fromDatasetId;
        sourceCol = fromCol;
        sourceTransform = fromTransform;
        lookupRole = toRole;
        lookupDatasetId = toDatasetId;
        lookupCol = toCol;
        lookupTransform = toTransform;
        mergeRole = toRole;
        mergeDatasetId = toDatasetId;
      } else if (!fromPresent && toPresent) {
        sourceRole = toRole;
        sourceDatasetId = toDatasetId;
        sourceCol = toCol;
        sourceTransform = toTransform;
        lookupRole = fromRole;
        lookupDatasetId = fromDatasetId;
        lookupCol = fromCol;
        lookupTransform = fromTransform;
        mergeRole = fromRole;
        mergeDatasetId = fromDatasetId;
      } else if (fromPresent && toPresent) {
        continue;
      } else {
        counters.joinSkippedMissingFromRole += 1;
        logComposeJoinProbeOnce({
          logger,
          loggedRef: loggedJoinProbeRef,
          customerId,
          ptrsId,
          message:
            "PTRS v2 composeMappedRowsForPtrs: join probe (neither side present on row; skipping)",
          meta: {
            join: j,
            fromRole,
            toRole,
          },
        });
        continue;
      }

      if (lookupDatasetId === String(transactionDatasetId)) {
        throw new Error(
          "Invalid join target: the selected transaction dataset cannot be a lookup dataset",
        );
      }

      const lhsVal = getJoinLhsValue(
        workingRow,
        sourceDatasetId,
        sourceCol,
      );
      const key = normalizeJoinKeyValue(lhsVal, sourceTransform);

      if (!key) {
        counters.joinNoKey += 1;
        logComposeJoinProbeOnce({
          logger,
          loggedRef: loggedJoinProbeRef,
          customerId,
          ptrsId,
          message: "PTRS v2 composeMappedRowsForPtrs: join probe (no key)",
          meta: {
            join: j,
            sourceRole,
            sourceCol,
            rawValue: lhsVal,
            normalisedKey: key,
          },
        });
        continue;
      }

      counters.joinIndexLookups += 1;
      const preparedKey = joinIndexKey(
        lookupDatasetId,
        lookupCol,
        lookupTransform,
      );
      const idx = preparedJoinIndexes.get(preparedKey) || new Map();

      const joined = idx.get(key);

      if (joined) {
        counters.joinMatched += 1;
        workingRow = mergeRoleRowNamespaced(
          workingRow,
          mergeDatasetId,
          joined,
        );
        presentDatasetIds.add(mergeDatasetId);

        logComposeJoinProbeOnce({
          logger,
          loggedRef: loggedJoinProbeRef,
          customerId,
          ptrsId,
          message: "PTRS v2 composeMappedRowsForPtrs: join probe (matched)",
          meta: {
            join: j,
            sourceRole,
            sourceCol,
            lookupRole,
            lookupCol,
            mergeRole,
            rawValue: lhsVal,
            normalisedKey: key,
            joinedKeys: Object.keys(joined || {}),
          },
        });
      } else {
        counters.joinNoMatch += 1;
        logComposeJoinProbeOnce({
          logger,
          loggedRef: loggedJoinProbeRef,
          customerId,
          ptrsId,
          message: "PTRS v2 composeMappedRowsForPtrs: join probe (no match)",
          meta: {
            join: j,
            sourceRole,
            sourceCol,
            lookupRole,
            lookupCol,
            mergeRole,
            rawValue: lhsVal,
            normalisedKey: key,
          },
        });
      }
    }

    srcRow = workingRow;
  }

  const hasCanonicalFieldMap =
    Array.isArray(fieldMapRows) && fieldMapRows.length > 0;

  if (!hasCanonicalFieldMap) {
    const e = new Error(
      "Mapped dataset build requires canonical field mappings; legacy support-config mappings are no longer supported.",
    );
    e.statusCode = 400;
    throw e;
  }

  let out = { ...(srcRow && typeof srcRow === "object" ? srcRow : {}) };

  if (Array.isArray(customFields) && customFields.length) {
    counters.customFieldsApplied += 1;
  }

  out.row_no = rawRow.rowNo;

  if (hasCanonicalFieldMap) {
    counters.canonicalProjectionApplied += 1;
    out = applyCanonicalProjectionForCompose({
      out,
      srcRow,
      fieldMapRows,
      resolveCanonicalValue,
      applyTransform,
      setCanonicalSourceMeta,
      counters,
    });
  }
  out = applyCanonicalInvoiceDatePolicy({ row: out, adapterType });
  return attachCanonicalSourceLineage({
    row: out,
    rawRow,
    transactionDatasetId,
  });
}

async function composeMappedRowsForPtrs({
  customerId,
  ptrsId,
  datasetId,
  limit = 50,
  offset = 0,
  afterRowNo = null,
  transaction = null,
  trace = null,
  hrMsSince,
  parseDateFlexible,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!datasetId) throw new Error("datasetId is required");
  if (typeof hrMsSince !== "function") {
    throw new Error("hrMsSince is required");
  }
  if (typeof parseDateFlexible !== "function") {
    throw new Error("parseDateFlexible is required");
  }

  const composeStartNs = process.hrtime.bigint();

  const stageStart = (name) => ({
    name,
    startNs: process.hrtime.bigint(),
  });

  const stageEnd = (s, extra = {}) => {
    if (!s) return;
    trace?.write("compose_stage_end", {
      stage: s.name,
      durationMs: hrMsSince(s.startNs),
      ...extra,
    });
  };

  trace?.write("compose_begin", { datasetId, limit, offset, afterRowNo });

  const transactionDataset = await resolveTransactionDatasetForCompose({
    customerId,
    ptrsId,
    datasetId,
    transaction,
    stageStart,
    stageEnd,
  });

  const { supportConfig, fieldMapRows } = await loadComposeDependencies({
    customerId,
    ptrsId,
    datasetId,
    transaction,
    trace,
    stageStart,
    stageEnd,
  });

  const { normalisedJoins } = normaliseConfiguredJoins({
    supportConfig,
    customerId,
    ptrsId,
    trace,
  });

  const customFields = normaliseConfiguredCustomFields({
    supportConfig,
    customerId,
    ptrsId,
    trace,
  });

  trace?.write("compose_custom_fields_loaded", {
    customFieldsCount: Array.isArray(customFields) ? customFields.length : 0,
    customFieldTargets: Array.isArray(customFields)
      ? customFields
          .map((cf) => cf?.key || cf?.field || cf?.target || cf?.name || null)
          .filter(Boolean)
          .slice(0, 20)
      : [],
    customFieldTypes: Array.isArray(customFields)
      ? customFields
          .map((cf) => String(cf?.type || ""))
          .filter(Boolean)
          .slice(0, 20)
      : [],
  });

  const orderedJoins = orderJoinsForTransactionDataset(
    normalisedJoins,
    datasetId,
  );
  trace?.write("compose_joins_ordered", {
    orderedJoinsCount: orderedJoins.length,
    configuredJoinsCount: normalisedJoins.length,
  });

  const _toNum = (v) => {
    if (v == null || v === "") return null;
    const s = String(v)
      .replace(/\$/g, "")
      .replace(/[\s,]+/g, "")
      .trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  };

  const applyTransform = ({ value, transformType }) => {
    const tt = (transformType || "").toString().trim().toLowerCase();
    if (!tt) return value;

    if (tt === "abs" || tt === "absolute" || tt === "absolute_numeric") {
      const n = _toNum(value);
      return n == null ? null : Math.abs(n);
    }

    if (tt === "trim") {
      return value == null ? null : String(value).trim();
    }

    if (tt === "date" || tt === "date_yyyy_mm_dd") {
      const d = parseDateFlexible(value);
      if (!d) return null;
      const yyyy = d.getUTCFullYear();
      const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(d.getUTCDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}`;
    }

    return value;
  };

  const nsKey = (sourceDatasetId, col) =>
    `${String(sourceDatasetId)}__${String(col)}`;

  const resolveCanonicalValue = ({
    sourceRole,
    sourceDatasetId,
    sourceColumn,
    srcRow,
    outRow,
  }) => {
    const col = sourceColumn;
    if (!col) return null;

    const resolvedSourceDatasetId = String(
      sourceDatasetId ||
        (String(sourceRole || "").toLowerCase() === "transaction"
          ? datasetId
          : ""),
    ).trim();
    const colSnake = toSnake(col);

    const candidateKeys =
      resolvedSourceDatasetId && resolvedSourceDatasetId !== String(datasetId)
        ? [
            nsKey(resolvedSourceDatasetId, col),
            colSnake ? nsKey(resolvedSourceDatasetId, colSnake) : null,
          ].filter(Boolean)
        : [col, colSnake].filter(Boolean);

    for (const key of candidateKeys) {
      const fromOut = pickFromRowLoose(outRow, key);
      if (fromOut != null && String(fromOut).trim() !== "") return fromOut;
    }

    for (const key of candidateKeys) {
      const fromSrc = pickFromRowLoose(srcRow, key);
      if (fromSrc != null && String(fromSrc).trim() !== "") return fromSrc;
    }

    return null;
  };

  const setCanonicalSourceMeta = ({
    outRow,
    canonicalField,
    sourceRole,
    sourceDatasetId,
    sourceColumn,
    transformType = null,
  }) => {
    if (!outRow || !canonicalField || !sourceRole || !sourceColumn) {
      return outRow;
    }

    const next = { ...(outRow || {}) };
    const meta =
      next._ptrsMeta && typeof next._ptrsMeta === "object"
        ? { ...next._ptrsMeta }
        : {};
    const canonicalSources =
      meta.canonicalSources && typeof meta.canonicalSources === "object"
        ? { ...meta.canonicalSources }
        : {};

    canonicalSources[String(canonicalField)] = {
      sourceRole: String(sourceRole),
      sourceDatasetId: sourceDatasetId ? String(sourceDatasetId) : null,
      sourceColumn: String(sourceColumn),
      transformType: transformType ? String(transformType) : null,
    };

    meta.canonicalSources = canonicalSources;
    next._ptrsMeta = meta;
    return next;
  };

  const getJoinLhsValue = (row, sourceDatasetId, col) => {
    if (!row) return undefined;
    if (String(sourceDatasetId) === String(datasetId)) {
      return pickFromRowLoose(row, col);
    }
    return pickFromRowLoose(row, nsKey(sourceDatasetId, col));
  };

  const mergeRoleRowNamespaced = (row, sourceDatasetId, joined) => {
    if (!joined || typeof joined !== "object") return row;
    const out = { ...(row || {}) };
    for (const [k, v] of Object.entries(joined)) {
      if (k === "_ptrsSource") continue;
      out[nsKey(sourceDatasetId, k)] = v;
    }
    const joinedSource = joined._ptrsSource;
    if (joinedSource && typeof joinedSource === "object") {
      const meta =
        out._ptrsMeta && typeof out._ptrsMeta === "object"
          ? { ...out._ptrsMeta }
          : {};
      meta.joinedReferences = {
        ...(meta.joinedReferences || {}),
        [String(sourceDatasetId)]: { ...joinedSource },
      };
      out._ptrsMeta = meta;
    }
    return out;
  };

  const datasetById = new Map();

  const preloadDatasetIdsForCompose = async () => {
    const sPreload = stageStart("preload_dataset_ids");
    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      attributes: ["id", "role", "purpose", "referenceKind"],
      raw: true,
      transaction,
    });

    for (const ds of dsRows || []) {
      if (!ds?.id) continue;
      datasetById.set(String(ds.id), ds);
    }

    for (const join of normalisedJoins) {
      if (
        !datasetById.has(String(join.fromDatasetId)) ||
        !datasetById.has(String(join.toDatasetId))
      ) {
        const error = new Error(
          "Join references a dataset outside the current PTRS",
        );
        error.statusCode = 400;
        throw error;
      }
    }

    stageEnd(sPreload, {
      datasetCount: datasetById.size,
    });

    trace?.write("compose_dataset_ids_preloaded", {
      datasetCount: datasetById.size,
    });
  };

  const joinIndexCache = new Map();
  const datasetRowsCache = new Map();

  const joinIndexKey = (sourceDatasetId, column, transform) => {
    const op = transform?.op ? String(transform.op) : "";
    const arg = transform?.arg != null ? String(transform.arg) : "";
    return `${sourceDatasetId}|${column}|${op}|${arg}`;
  };

  const loadRowsForDataset = async (sourceDatasetId) => {
    const id = String(sourceDatasetId || "");
    if (!id) return [];
    if (datasetRowsCache.has(id)) return datasetRowsCache.get(id);

    if (!datasetById.has(id)) {
      datasetRowsCache.set(id, []);
      return [];
    }

    const where = { customerId, ptrsId, datasetId: id };

    const rows = await db.PtrsImportRaw.findAll({
      where,
      order: [["rowNo", "ASC"]],
      attributes: ["id", "rowNo", "data"],
      raw: true,
      transaction,
    });

    const parsed = (rows || []).map((x) => {
      let d = x?.data || {};
      if (typeof d === "string") {
        try {
          d = JSON.parse(d);
        } catch {
          d = {};
        }
      }

      const baseRow = {
        ...(d && typeof d === "object" ? d : {}),
        _ptrsSource: {
          sourceDatasetId: id,
          sourceRawRowId: x?.id || null,
          sourceRowNo: Number(x?.rowNo),
        },
      };

      return Array.isArray(customFields) && customFields.length
        ? applyCustomFields({
            row: baseRow,
            rawRow: baseRow,
            customFields,
            rowDatasetId: id,
          })
        : baseRow;
    });

    datasetRowsCache.set(id, parsed);
    return parsed;
  };

  const getJoinIndex = async ({ sourceDatasetId, column, transform }) => {
    const id = String(sourceDatasetId || "");
    if (!id) return new Map();
    if (id === String(datasetId)) {
      throw new Error(
        "Cannot build a lookup index for the selected transaction dataset",
      );
    }

    const cacheKey = joinIndexKey(id, column, transform);
    if (joinIndexCache.has(cacheKey)) return joinIndexCache.get(cacheKey);

    const sIdx = stageStart("build_join_index");
    const rows = await loadRowsForDataset(id);
    const idx = new Map();

    for (const row of rows) {
      const rawVal = pickFromRowLoose(row, column);
      const k = normalizeJoinKeyValue(rawVal, transform);
      if (!k) continue;
      if (!idx.has(k)) idx.set(k, row);
    }

    stageEnd(sIdx, {
      datasetId: id,
      column,
      transform: transform || null,
      rowsScanned: Array.isArray(rows) ? rows.length : 0,
      indexSize: idx.size,
    });

    joinIndexCache.set(cacheKey, idx);
    return idx;
  };

  const prebuildJoinIndexes = async (joinsToPrepare) => {
    const sPrep = stageStart("prebuild_join_indexes");
    const prepared = new Map();
    const specs = [];
    const seen = new Set();

    for (const j of joinsToPrepare || []) {
      if (!j || typeof j !== "object") continue;

      const candidates = [
        {
          sourceDatasetId: String(j.toDatasetId || ""),
          column: j.toColumn,
          transform: j.toTransform || null,
        },
        {
          sourceDatasetId: String(j.fromDatasetId || ""),
          column: j.fromColumn,
          transform: j.fromTransform || null,
        },
      ];

      for (const spec of candidates) {
        if (
          !spec.sourceDatasetId ||
          !spec.column ||
          spec.sourceDatasetId === String(datasetId)
        ) {
          continue;
        }
        const key = joinIndexKey(
          spec.sourceDatasetId,
          spec.column,
          spec.transform,
        );
        if (seen.has(key)) continue;
        seen.add(key);
        specs.push({ ...spec, cacheKey: key });
      }
    }

    for (const spec of specs) {
      const idx = await getJoinIndex({
        sourceDatasetId: spec.sourceDatasetId,
        column: spec.column,
        transform: spec.transform,
      });
      prepared.set(spec.cacheKey, idx);
    }

    stageEnd(sPrep, {
      preparedIndexCount: prepared.size,
      preparedSpecsCount: specs.length,
    });

    trace?.write("compose_join_indexes_prebuilt", {
      preparedIndexCount: prepared.size,
      preparedSpecsCount: specs.length,
    });

    return prepared;
  };

  const transactionRows = await loadTransactionRowsForCompose({
    customerId,
    ptrsId,
    datasetId: transactionDataset.id,
    limit,
    offset,
    afterRowNo,
    transaction,
    stageStart,
    stageEnd,
  });

  await preloadDatasetIdsForCompose();
  const preparedJoinIndexes = await prebuildJoinIndexes(orderedJoins);

  const loopStartNs = process.hrtime.bigint();

  const counters = {
    rowsInput: Array.isArray(transactionRows) ? transactionRows.length : 0,
    joinsOrdered: Array.isArray(orderedJoins) ? orderedJoins.length : 0,
    joinAttempts: 0,
    joinSkippedMissingFromRole: 0,
    joinNoKey: 0,
    joinIndexLookups: 0,
    joinMatched: 0,
    joinNoMatch: 0,
    customFieldsApplied: 0,
    canonicalProjectionApplied: 0,
    canonicalSourceMetaApplied: 0,
  };

  const composed = [];

  const loggedJoinProbeRef = { logged: false };

  for (const r of transactionRows) {
    const out = await composeSingleMappedRow({
      rawRow: r,
      orderedJoins,
      customFields,
      fieldMapRows,
      preparedJoinIndexes,
      counters,
      customerId,
      ptrsId,
      logger,
      loggedJoinProbeRef,
      transactionDatasetId: datasetId,
      adapterType: transactionDataset.adapterType,
      getJoinLhsValue,
      mergeRoleRowNamespaced,
      joinIndexKey,
      resolveCanonicalValue,
      applyTransform,
      setCanonicalSourceMeta,
      normalizeJoinKeyValue,
      logComposeJoinProbeOnce,
    });
    composed.push(out);
  }

  trace?.write("compose_loop_complete", {
    durationMs: hrMsSince(loopStartNs),
    ...counters,
  });

  const headers = buildHeadersFromComposedRows(composed);

  trace?.write("compose_headers_built", {
    headersCount: Array.isArray(headers) ? headers.length : 0,
  });

  trace?.write("compose_end", {
    rowsOut: Array.isArray(composed) ? composed.length : 0,
    totalMs: hrMsSince(composeStartNs),
  });

  return { rows: composed, headers };
}

module.exports = {
  orderJoinsForTransactionDataset,
  applyCustomFields,
  applyCanonicalProjectionForCompose,
  attachCanonicalSourceLineage,
  composeSingleMappedRow,
  composeMappedRowsForPtrs,
};
