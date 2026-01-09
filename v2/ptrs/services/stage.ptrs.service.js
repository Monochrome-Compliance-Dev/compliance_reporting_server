const db = require("@/db/database");
const { pickFromRowLoose } = require("./data.ptrs.service");

const {
  safeMeta,
  slog,
  buildStableInputHash,
  createExecutionRun,
  getLatestExecutionRun,
  updateExecutionRun,
} = require("./ptrs.service");

const { applyRules } = require("./rules.ptrs.service");
const {
  loadMappedRowsForPtrs,
  getColumnMap,
} = require("./tablesAndMaps.ptrs.service");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  stagePtrs,
  getStagePreview,
};

function normaliseFieldMapRows(fieldMapRows) {
  if (!Array.isArray(fieldMapRows)) return [];
  return fieldMapRows
    .filter((r) => r && typeof r === "object")
    .map((r) => ({
      canonicalField: r.canonicalField,
      sourceRole: r.sourceRole,
      sourceColumn: r.sourceColumn,
      transformType: r.transformType,
      transformConfig: r.transformConfig,
    }))
    .filter((r) => r.canonicalField && r.sourceRole);
}

function applyFieldMapToRow(row, fieldMap) {
  // Preserve internal/meta fields we rely on
  const out = {};
  if (row && typeof row === "object") {
    for (const [k, v] of Object.entries(row)) {
      if (k === "row_no" || k === "rowNo" || k.startsWith("_")) {
        out[k] = v;
      }
    }
  }

  if (!row || typeof row !== "object") return out;

  for (const m of fieldMap) {
    const key = m.canonicalField;

    // Prefer explicit sourceColumn; fall back to canonicalField name.
    const header = m.sourceColumn || key;

    // Loose pick (handles case/spacing/snake variants)
    const val = pickFromRowLoose(row, header);

    // Keep key present even if missing (null), so metrics/reporting can detect gaps.
    out[key] = val != null ? val : null;
  }

  return out;
}

function applyFieldMapToRows(rows, fieldMap) {
  if (!Array.isArray(rows) || !rows.length) return rows || [];
  if (!Array.isArray(fieldMap) || !fieldMap.length) return rows;
  return rows.map((r) => applyFieldMapToRow(r, fieldMap));
}

/**
 * Stage data for a ptrs. Reuses previewTransform pipeline to project/optionally filter, then
 * (when persist=true) writes rows into tbl_ptrs_stage_row and updates ptrs status.
 * Returns { sample, affectedCount, persistedCount? }.
 * RLS-aware: runs in beginTransactionWithCustomerContext and passes transaction to all DB calls.
 */
async function stagePtrs({
  customerId,
  ptrsId,
  steps = [],
  persist = false,
  limit = null,
  userId,
  profileId = null,
}) {
  let executionRun = null;
  let inputHash = null;

  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // Persist runs must be attributable to a profile.
  if (persist && !profileId) {
    const e = new Error("profileId is required when persist=true");
    e.statusCode = 400;
    throw e;
  }

  const started = Date.now();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // --- Execution run tracking (only for persist runs) ---

    if (persist) {
      // Hash inputs that materially affect staging.
      // We intentionally hash metadata/config, not full row data.
      const [
        mapRow,
        rawCount,
        rawMaxUpdatedAt,
        datasets,
        fieldMapUpdatedAt,
        fieldMapCount,
      ] = await Promise.all([
        getColumnMap({ customerId, ptrsId, transaction: t }),
        db.PtrsImportRaw.count({
          where: { customerId, ptrsId },
          transaction: t,
        }),
        db.PtrsImportRaw.max("updatedAt", {
          where: { customerId, ptrsId },
          transaction: t,
        }),
        db.PtrsRawDataset
          ? db.PtrsRawDataset.findAll({
              where: { customerId, ptrsId },
              attributes: ["id", "role", "updatedAt"],
              order: [
                ["role", "ASC"],
                ["updatedAt", "DESC"],
              ],
              transaction: t,
              raw: true,
            })
          : Promise.resolve([]),
        db.PtrsFieldMap
          ? db.PtrsFieldMap.max("updatedAt", {
              where: { customerId, ptrsId, profileId },
              transaction: t,
            })
          : Promise.resolve(null),
        db.PtrsFieldMap
          ? db.PtrsFieldMap.count({
              where: { customerId, ptrsId, profileId },
              transaction: t,
            })
          : Promise.resolve(0),
      ]);

      inputHash = buildStableInputHash({
        ptrsId,
        customerId,
        profileId: profileId || null,
        map: mapRow
          ? {
              id: mapRow.id || null,
              updatedAt: mapRow.updatedAt || null,
              mappings: mapRow.mappings || null,
              joins: mapRow.joins || null,
              customFields: mapRow.customFields || null,
              rowRules: mapRow.rowRules || null,
            }
          : null,
        fieldMap: {
          profileId: profileId || null,
          count: Number(fieldMapCount) || 0,
          maxUpdatedAt: fieldMapUpdatedAt || null,
        },
        importRaw: {
          rowCount: rawCount || 0,
          maxUpdatedAt: rawMaxUpdatedAt || null,
        },
        datasets: Array.isArray(datasets)
          ? datasets.map((d) => ({
              id: d.id,
              role: d.role,
              updatedAt: d.updatedAt || null,
            }))
          : [],
      });

      const previous = await getLatestExecutionRun({
        customerId,
        ptrsId,
        step: "stage",
        transaction: t,
      });

      slog.info("PTRS v2 stagePtrs: execution input hash", {
        action: "PtrsV2StagePtrsInputHash",
        customerId,
        ptrsId,
        profileId: profileId || null,
        inputHash,
        previousHash: previous?.inputHash || null,
        rawCount: rawCount || 0,
        datasetsCount: Array.isArray(datasets) ? datasets.length : 0,
      });

      executionRun = await createExecutionRun({
        customerId,
        ptrsId,
        profileId,
        step: "stage",
        inputHash,
        status: "running",
        startedAt: new Date(),
        createdBy: userId || null,
        transaction: t,
      });
    }

    // 1) Compose mapped rows for this ptrs (import + joins + column map)
    const { rows: baseRows } = await loadMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: null,
      transaction: t,
    });

    // Canonical field mapping: if a profile-scoped field map exists, project rows to canonical fields.
    let fieldMap = [];
    if (profileId && db.PtrsFieldMap) {
      const fmRows = await db.PtrsFieldMap.findAll({
        where: { customerId, ptrsId, profileId },
        order: [["canonicalField", "ASC"]],
        transaction: t,
        raw: true,
      });
      fieldMap = normaliseFieldMapRows(fmRows);
    }

    const projectedRows = applyFieldMapToRows(baseRows, fieldMap);

    slog.info("PTRS v2 stagePtrs: loaded mapped rows", {
      action: "PtrsV2StagePtrsLoadedMappedRows",
      customerId,
      ptrsId,
      rowsCount: Array.isArray(projectedRows) ? projectedRows.length : 0,
      sampleRowKeys:
        projectedRows && projectedRows[0]
          ? Object.keys(projectedRows[0])
          : null,
    });

    // 2) Apply row-level rules (if any) independently of preview
    let rows = projectedRows;
    let rulesStats = null;

    try {
      let rowRules = null;
      try {
        const mapRow = await getColumnMap({
          customerId,
          ptrsId,
          transaction: t,
        });
        rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
        if (typeof rowRules === "string") {
          try {
            rowRules = JSON.parse(rowRules);
          } catch {
            rowRules = null;
          }
        }
      } catch (_) {
        rowRules = null;
      }

      const rulesResult = applyRules(
        rows,
        Array.isArray(rowRules) ? rowRules : []
      );
      rows = rulesResult.rows || rows;
      rulesStats = rulesResult.stats || null;
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to apply row rules", {
        action: "PtrsV2StagePtrsApplyRules",
        customerId,
        ptrsId,
        error: err.message,
      });
    }

    // 3) Persist into tbl_ptrs_stage_row if requested
    let persistedCount = null;
    if (persist) {
      const basePayload = rows.map((r) => {
        const rowNoVal = Number(r?.row_no ?? r?.rowNo ?? 0) || 0;

        // Persist the full resolved row into JSONB `data`.
        // NOTE: tbl_ptrs_stage_row only has: customerId, ptrsId, rowNo, data, errors, meta (+ timestamps)
        const dataObj =
          r && typeof r === "object" && Object.keys(r).length
            ? r
            : { _warning: "⚠️ No mapped data for this row" };

        return {
          customerId: String(customerId),
          ptrsId: String(ptrsId),
          rowNo: rowNoVal,
          data: dataObj,
          errors: null,
          meta: {
            _stage: "ptrs.v2.stagePtrs",
            at: new Date().toISOString(),
            rules: {
              applied: Array.isArray(r?._appliedRules) ? r._appliedRules : [],
              exclude: !!r?.exclude,
            },
          },
        };
      });

      const isEmptyPlain = (v) =>
        v &&
        typeof v === "object" &&
        !Array.isArray(v) &&
        Object.keys(v).length === 0;

      const insertWarning = (obj) => {
        if (!obj || typeof obj !== "object") return obj;
        for (const key of ["data", "errors", "meta"]) {
          if (isEmptyPlain(obj[key])) {
            obj[key] = {
              _warning: "⚠️ Empty JSONB payload — nothing to insert",
            };
          }
          if (typeof obj[key] === "undefined") {
            obj[key] = null;
          }
        }
        return obj;
      };

      const safePayload = basePayload.map(insertWarning);

      slog.info("PTRS v2 stagePtrs: preparing to insert", {
        action: "PtrsV2StagePtrsBatch",
        customerId,
        ptrsId,
        batchSize: safePayload.length,
        sampleRow: safeMeta(safePayload[0] || {}),
      });

      const offenders = safePayload
        .filter((p) => {
          const hasWarn = Boolean(
            p?.data?._warning || p?.errors?._warning || p?.meta?._warning
          );
          const hasEmpty =
            isEmptyPlain(p?.data) ||
            isEmptyPlain(p?.errors) ||
            isEmptyPlain(p?.meta);
          return hasWarn || hasEmpty;
        })
        .slice(0, 3)
        .map((p) => ({
          rowNo: p.rowNo,
          dataKeys: p.data ? Object.keys(p.data) : null,
          hasWarning: Boolean(
            p?.data?._warning || p?.errors?._warning || p?.meta?._warning
          ),
        }));

      if (offenders.length) {
        slog.warn("PTRS v2 stagePtrs: warning/empty JSONB rows detected", {
          action: "PtrsV2StagePtrsWarningRows",
          ptrsId,
          customerId,
          offenderCount: offenders.length,
          sample: safeMeta(offenders),
        });
      }

      await db.PtrsStageRow.destroy({
        where: { customerId, ptrsId },
        transaction: t,
      });

      if (safePayload.length) {
        try {
          await db.PtrsStageRow.bulkCreate(safePayload, {
            validate: false,
            returning: false,
            transaction: t,
          });
        } catch (e) {
          // If RLS blocks inserts, or the model/table are out of sync, we want this to be unmistakable.
          slog.error("PTRS v2 stagePtrs: bulkCreate failed", {
            action: "PtrsV2StagePtrsBulkCreateFailed",
            customerId,
            ptrsId,
            error: e?.message,
          });
          throw e;
        }
      }

      persistedCount = await db.PtrsStageRow.count({
        where: { customerId, ptrsId },
        transaction: t,
      });

      slog.info("PTRS v2 stagePtrs: persistence check", {
        action: "PtrsV2StagePtrsPersistedCount",
        customerId,
        ptrsId,
        attempted: safePayload.length,
        persistedCount,
      });
    }

    const tookMs = Date.now() - started;
    // Ensure persistedCount is calculated before commit
    if (!persist) {
      persistedCount = null;
    }

    if (executionRun?.id) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "success",
          finishedAt: new Date(),
          rowsIn: rows.length,
          rowsOut: rows.length,
          stats: { rules: rulesStats },
          errorMessage: null,
          updatedBy: userId || null,
          transaction: t,
        });
      } catch (e) {
        slog.warn(
          "PTRS v2 stagePtrs: failed to update execution run (non-fatal)",
          {
            action: "PtrsV2StagePtrsUpdateExecutionRunFailed",
            customerId,
            ptrsId,
            executionRunId: executionRun.id,
            error: e?.message,
          }
        );
      }
    }

    await t.commit();

    return {
      rowsIn: rows.length,
      rowsOut: rows.length,
      persistedCount,
      tookMs,
      sample: rows[0] || null,
      stats: { rules: rulesStats },
    };
  } catch (err) {
    if (executionRun?.id) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "failed",
          finishedAt: new Date(),
          errorMessage: err?.message || "Stage failed",
          updatedBy: userId || null,
          transaction: t,
        });
      } catch (e) {
        // best-effort only
      }
    }

    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {
        // ignore rollback errors
      }
    }
    throw err;
  }
}

/**
 * Returns a preview of staged data using the persisted staging table
 * (tbl_ptrs_stage_row). We:
 *  - read a limited page of rows for preview
 *  - get a full count for this ptrsId
 * so the FE can show "20 of 208,811 rows".
 */
async function getStagePreview({
  customerId,
  ptrsId,
  limit = 50,
  profileId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  let canon = [];
  try {
    const where = { customerId, ptrsId };

    // Pull a preview page and a full count in parallel
    const [rowsRaw, totalRows] = await Promise.all([
      db.PtrsStageRow.findAll({
        where,
        order: [["rowNo", "ASC"]],
        limit,
        transaction: t,
      }),
      db.PtrsStageRow.count({ where, transaction: t }),
    ]);

    const rows = rowsRaw.map((r) =>
      typeof r.toJSON === "function" ? r.toJSON() : r
    );

    // Derive headers from all rows' JSONB payloads (data/standard/custom)
    const headerSet = new Set();

    const materialiseObj = (value) => {
      if (!value) return null;
      if (typeof value === "string") {
        try {
          const parsed = JSON.parse(value);
          return parsed && typeof parsed === "object" ? parsed : null;
        } catch {
          return null;
        }
      }
      if (typeof value === "object") return value;
      return null;
    };

    // If a field map exists for this profile, prefer its canonical fields as the primary header list.
    if (profileId && db.PtrsFieldMap) {
      try {
        const fm = await db.PtrsFieldMap.findAll({
          where: { customerId, ptrsId, profileId },
          attributes: ["canonicalField"],
          order: [["canonicalField", "ASC"]],
          transaction: t,
          raw: true,
        });

        canon = Array.isArray(fm)
          ? fm.map((r) => r.canonicalField).filter(Boolean)
          : [];

        if (canon.length) {
          canon.forEach((k) => headerSet.add(k));
        }
      } catch (_) {
        // ignore
      }
    }

    for (const row of rows) {
      if (!row) continue;
      const buckets = [row.data];
      for (const bucket of buckets) {
        const obj = materialiseObj(bucket);
        if (!obj) continue;
        Object.keys(obj).forEach((k) => headerSet.add(k));
      }
    }

    // Remove the post-row field map header block (now handled above)

    // Order headers: canonical fields first (if present), then discovered fields
    let headers;
    if (canon.length) {
      const canonSet = new Set(canon);
      headers = [
        ...canon.filter((h) => headerSet.has(h)),
        ...Array.from(headerSet).filter((h) => !canonSet.has(h)),
      ];
    } else {
      headers = Array.from(headerSet);
    }

    await t.commit();

    return {
      headers,
      rows,
      totalRows,
      stats: null,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch {
        // ignore rollback errors
      }
    }
    throw err;
  }
}
