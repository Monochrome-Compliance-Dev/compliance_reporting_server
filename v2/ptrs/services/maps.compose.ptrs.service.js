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
  resolveMainDatasetForCompose,
  loadMainRowsForCompose,
  buildHeadersFromComposedRows,
} = require("@/v2/ptrs/services/maps.dependencies.ptrs.service");

function applyCustomFields({ row, rawRow, customFields }) {
  const out = { ...(row || {}) };
  const source = rawRow && typeof rawRow === "object" ? rawRow : {};

  const isMainRole = (role) => {
    const r = String(role || "")
      .trim()
      .toLowerCase();
    return !r || r === "main" || r.startsWith("main_");
  };

  const nsKey = (role, col) => `${String(role)}__${String(col)}`;

  for (const cf of Array.isArray(customFields) ? customFields : []) {
    if (!cf || typeof cf !== "object") continue;

    const target = cf.field || cf.target || cf.name;
    if (!target) continue;

    let value = null;

    if (Object.prototype.hasOwnProperty.call(cf, "value")) {
      value = cf.value;
    } else {
      const sourceRole = String(cf.sourceRole || cf.role || "")
        .trim()
        .toLowerCase();

      const sourceColumn =
        cf.sourceColumn || cf.column || cf.sourceHeader || cf.header || null;

      if (!sourceColumn) continue;

      if (isMainRole(sourceRole)) {
        value =
          pickFromRowLoose(out, sourceColumn) ??
          pickFromRowLoose(source, sourceColumn);
      } else {
        value =
          pickFromRowLoose(out, nsKey(sourceRole, sourceColumn)) ??
          pickFromRowLoose(source, nsKey(sourceRole, sourceColumn));
      }
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
  isMainRole,
  hasRoleInRow,
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
  let srcRow = base;

  if (orderedJoins.length) {
    let workingRow = srcRow;

    for (const j of orderedJoins) {
      counters.joinAttempts += 1;
      const fromRole = String(j.fromRole || "").toLowerCase();
      const toRole = String(j.toRole || "").toLowerCase();

      const fromCol = j.fromColumn;
      const toCol = j.toColumn;

      const fromTransform = j.fromTransform || null;
      const toTransform = j.toTransform || null;

      if (!fromRole || !toRole || !fromCol || !toCol) continue;

      const fromPresent =
        isMainRole(fromRole) || hasRoleInRow(workingRow, fromRole);
      const toPresent = isMainRole(toRole) || hasRoleInRow(workingRow, toRole);

      let sourceRole = null;
      let sourceCol = null;
      let sourceTransform = null;
      let lookupRole = null;
      let lookupCol = null;
      let lookupTransform = null;
      let mergeRole = null;

      if (fromPresent && !toPresent) {
        sourceRole = fromRole;
        sourceCol = fromCol;
        sourceTransform = fromTransform;
        lookupRole = toRole;
        lookupCol = toCol;
        lookupTransform = toTransform;
        mergeRole = toRole;
      } else if (!fromPresent && toPresent) {
        sourceRole = toRole;
        sourceCol = toCol;
        sourceTransform = toTransform;
        lookupRole = fromRole;
        lookupCol = fromCol;
        lookupTransform = fromTransform;
        mergeRole = fromRole;
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

      if (isMainRole(lookupRole)) {
        throw new Error(
          `Invalid join target: lookupRole '${lookupRole}' must be a supporting dataset role`,
        );
      }

      const lhsVal = getJoinLhsValue(workingRow, sourceRole, sourceCol);
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
      const preparedKey = joinIndexKey(lookupRole, lookupCol, lookupTransform);
      const idx = preparedJoinIndexes.get(preparedKey) || new Map();

      const joined = idx.get(key);

      if (joined) {
        counters.joinMatched += 1;
        workingRow = mergeRoleRowNamespaced(workingRow, mergeRole, joined);

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
    out = applyCustomFields({
      row: out,
      rawRow: srcRow,
      customFields,
    });
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

  return out;
}

async function composeMappedRowsForPtrs({
  customerId,
  ptrsId,
  limit = 50,
  offset = 0,
  transaction = null,
  trace = null,
  hrMsSince,
  parseDateFlexible,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
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

  trace?.write("compose_begin", { limit, offset });

  const { supportConfig, fieldMapRows } = await loadComposeDependencies({
    customerId,
    ptrsId,
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

  const orderJoinsForExecution = (joins) => {
    const list = Array.isArray(joins) ? joins.slice() : [];
    if (list.length <= 1) return list;

    const norm = (r) =>
      String(r || "")
        .trim()
        .toLowerCase();

    const available = new Set(["main"]);
    let remaining = list.slice();
    const ordered = [];

    let guard = 0;
    while (remaining.length) {
      guard += 1;
      if (guard > list.length + 10) break;

      const passPicked = [];
      const passLeft = [];

      for (const j of remaining) {
        const fromRole = norm(j.fromRole);
        const toRole = norm(j.toRole);

        if (!fromRole || !toRole) {
          passPicked.push(j);
          continue;
        }

        const fromAvailable = fromRole === "main" || available.has(fromRole);
        const toAvailable = toRole === "main" || available.has(toRole);

        if (fromAvailable || toAvailable) {
          passPicked.push(j);
        } else {
          passLeft.push(j);
        }
      }

      if (!passPicked.length) {
        const rolesKnown = Array.from(available);
        const missing = Array.from(
          new Set(
            passLeft
              .flatMap((j) => [norm(j.fromRole), norm(j.toRole)])
              .filter((r) => r && r !== "main" && !available.has(r)),
          ),
        );

        const e = new Error(
          `Invalid join dependency chain: cannot resolve join order. ` +
            `Roles available: ${rolesKnown.join(", ") || "(none)"}. ` +
            `Missing/blocked roles: ${missing.join(", ") || "(unknown)"}.`,
        );
        e.statusCode = 400;
        throw e;
      }

      for (const j of passPicked) {
        ordered.push(j);
        const fromRole = norm(j.fromRole);
        const toRole = norm(j.toRole);
        if (fromRole) available.add(fromRole);
        if (toRole) available.add(toRole);
      }

      remaining = passLeft;
    }

    return ordered;
  };

  const orderedJoins = orderJoinsForExecution(normalisedJoins);
  trace?.write("compose_joins_ordered", {
    orderedJoinsCount: orderedJoins.length,
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

  const isMainRole = (role) => {
    const r = String(role || "").toLowerCase();
    return r === "main" || r.startsWith("main_");
  };

  const nsKey = (role, col) => `${String(role)}__${String(col)}`;

  const resolveCanonicalValue = ({
    sourceRole,
    sourceColumn,
    srcRow,
    outRow,
  }) => {
    const col = sourceColumn;
    if (!col) return null;

    const role = String(sourceRole || "")
      .trim()
      .toLowerCase();
    const colSnake = toSnake(col);

    const candidateKeys =
      role && !isMainRole(role)
        ? [nsKey(role, col), colSnake ? nsKey(role, colSnake) : null].filter(
            Boolean,
          )
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
      sourceColumn: String(sourceColumn),
      transformType: transformType ? String(transformType) : null,
    };

    meta.canonicalSources = canonicalSources;
    next._ptrsMeta = meta;
    return next;
  };

  const getJoinLhsValue = (row, role, col) => {
    if (!row) return undefined;
    const r = String(role || "").toLowerCase();
    if (isMainRole(r)) {
      return pickFromRowLoose(row, col);
    }
    return pickFromRowLoose(row, nsKey(r, col));
  };

  const mergeRoleRowNamespaced = (row, role, joined) => {
    const r = String(role || "").toLowerCase();
    if (!joined || typeof joined !== "object") return row;
    const out = { ...(row || {}) };
    for (const [k, v] of Object.entries(joined)) {
      out[nsKey(r, k)] = v;
    }
    return out;
  };

  const hasRoleInRow = (row, role) => {
    const r = String(role || "").toLowerCase();
    if (isMainRole(r)) return true;
    const prefix = `${r}__`;
    return Object.keys(row || {}).some((k) => String(k).startsWith(prefix));
  };

  const datasetIdByRole = new Map();

  const preloadDatasetIdsForCompose = async () => {
    const sPreload = stageStart("preload_dataset_ids");
    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      attributes: ["id", "role"],
      raw: true,
      transaction,
    });

    for (const ds of dsRows || []) {
      const role = String(ds?.role || "")
        .trim()
        .toLowerCase();
      if (!role || !ds?.id) continue;
      if (!datasetIdByRole.has(role)) {
        datasetIdByRole.set(role, ds.id);
      }
    }

    stageEnd(sPreload, {
      datasetRoleCount: datasetIdByRole.size,
    });

    trace?.write("compose_dataset_ids_preloaded", {
      datasetRoleCount: datasetIdByRole.size,
    });
  };

  const joinIndexCache = new Map();
  const datasetRowsCache = new Map();

  const joinIndexKey = (role, column, transform) => {
    const op = transform?.op ? String(transform.op) : "";
    const arg = transform?.arg != null ? String(transform.arg) : "";
    return `${role}|${column}|${op}|${arg}`;
  };

  const loadRowsForRole = async (role) => {
    const r = String(role || "").toLowerCase();
    if (!r) return [];
    if (datasetRowsCache.has(r)) return datasetRowsCache.get(r);

    const datasetId = datasetIdByRole.get(r) || null;

    if (!datasetId) {
      datasetRowsCache.set(r, []);
      return [];
    }

    const where = { customerId, datasetId };
    if (
      db.PtrsImportRaw.rawAttributes &&
      db.PtrsImportRaw.rawAttributes.ptrsDatasetId
    ) {
      delete where.datasetId;
      where.datasetId = datasetId;
    }

    const rows = await db.PtrsImportRaw.findAll({
      where,
      order: [["rowNo", "ASC"]],
      attributes: ["data"],
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
      return d && typeof d === "object" ? d : {};
    });

    datasetRowsCache.set(r, parsed);
    return parsed;
  };

  const getJoinIndex = async ({ role, column, transform }) => {
    const r = String(role || "").toLowerCase();
    if (!r) return new Map();
    if (isMainRole(r)) {
      throw new Error(
        `Invalid join target role '${r}' — cannot build an index for main roles`,
      );
    }

    const cacheKey = joinIndexKey(r, column, transform);
    if (joinIndexCache.has(cacheKey)) return joinIndexCache.get(cacheKey);

    const sIdx = stageStart("build_join_index");
    const rows = await loadRowsForRole(r);
    const idx = new Map();

    for (const row of rows) {
      const rawVal = pickFromRowLoose(row, column);
      const k = normalizeJoinKeyValue(rawVal, transform);
      if (!k) continue;
      if (!idx.has(k)) idx.set(k, row);
    }

    stageEnd(sIdx, {
      role: r,
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
          role: String(j.toRole || "").toLowerCase(),
          column: j.toColumn,
          transform: j.toTransform || null,
        },
        {
          role: String(j.fromRole || "").toLowerCase(),
          column: j.fromColumn,
          transform: j.fromTransform || null,
        },
      ];

      for (const spec of candidates) {
        if (!spec.role || !spec.column || isMainRole(spec.role)) continue;
        const key = joinIndexKey(spec.role, spec.column, spec.transform);
        if (seen.has(key)) continue;
        seen.add(key);
        specs.push({ ...spec, cacheKey: key });
      }
    }

    for (const spec of specs) {
      const idx = await getJoinIndex({
        role: spec.role,
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

  const mainDatasetId = await resolveMainDatasetForCompose({
    customerId,
    ptrsId,
    transaction,
    stageStart,
    stageEnd,
  });

  const mainRows = await loadMainRowsForCompose({
    customerId,
    ptrsId,
    mainDatasetId,
    limit,
    offset,
    transaction,
    stageStart,
    stageEnd,
  });

  await preloadDatasetIdsForCompose();
  const preparedJoinIndexes = await prebuildJoinIndexes(orderedJoins);

  const loopStartNs = process.hrtime.bigint();

  const counters = {
    rowsInput: Array.isArray(mainRows) ? mainRows.length : 0,
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

  let loggedFirst = false;
  const loggedJoinProbeRef = { logged: false };

  for (const r of mainRows) {
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
      isMainRole,
      hasRoleInRow,
      getJoinLhsValue,
      mergeRoleRowNamespaced,
      joinIndexKey,
      resolveCanonicalValue,
      applyTransform,
      setCanonicalSourceMeta,
      normalizeJoinKeyValue,
      logComposeJoinProbeOnce,
    });

    if (!loggedFirst && logger && logger.debug) {
      loggedFirst = true;
      slog.debug(
        "PTRS v2 composeMappedRowsForPtrs: sample composed row",
        safeMeta({
          customerId,
          ptrsId,
          sampleRowKeys: Object.keys(out || {}),
          hasCustomFieldsApplied:
            Array.isArray(customFields) && customFields.length > 0,
        }),
      );
    }

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
  applyCustomFields,
  applyCanonicalProjectionForCompose,
  composeSingleMappedRow,
  composeMappedRowsForPtrs,
};
