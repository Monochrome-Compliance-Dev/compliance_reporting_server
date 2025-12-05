const db = require("@/db/database");
const csv = require("fast-csv");
const path = require("path");
const { Readable } = require("stream");
const fs = require("fs");

const { safeMeta, slog } = require("./ptrs.service");
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
  limit = 50,
  userId,
  profileId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const started = Date.now();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // 1) Compose mapped rows for this ptrs (import + joins + column map)
    const { rows: baseRows } = await loadMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit,
      transaction: t,
    });

    // 2) Apply row-level rules (if any) independently of preview
    let rows = baseRows;
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
    if (persist) {
      const basePayload = rows.map((r) => {
        const rowNoVal = Number(r?.row_no ?? r?.rowNo ?? 0) || 0;
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
          standard: null,
          custom: null,
          meta: {
            _stage: "ptrs.v2.stagePtrs",
            at: new Date().toISOString(),
            rules: {
              applied: Array.isArray(r._appliedRules) ? r._appliedRules : [],
              exclude: !!r.exclude,
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
        for (const key of ["data", "errors", "standard", "custom", "meta"]) {
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
            p?.data?._warning ||
              p?.errors?._warning ||
              p?.standard?._warning ||
              p?.custom?._warning ||
              p?.meta?._warning
          );
          const hasEmpty =
            isEmptyPlain(p?.data) ||
            isEmptyPlain(p?.errors) ||
            isEmptyPlain(p?.standard) ||
            isEmptyPlain(p?.custom) ||
            isEmptyPlain(p?.meta);
          return hasWarn || hasEmpty;
        })
        .slice(0, 3)
        .map((p) => ({
          rowNo: p.rowNo,
          dataKeys: p.data ? Object.keys(p.data) : null,
          hasWarning: Boolean(
            p?.data?._warning ||
              p?.errors?._warning ||
              p?.standard?._warning ||
              p?.custom?._warning ||
              p?.meta?._warning
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
        await db.PtrsStageRow.bulkCreate(safePayload, {
          validate: false,
          returning: false,
          transaction: t,
        });
      }
    }

    const tookMs = Date.now() - started;
    await t.commit();

    return {
      rowsIn: rows.length,
      rowsOut: rows.length,
      tookMs,
      sample: rows[0] || null,
      stats: { rules: rulesStats },
    };
  } catch (err) {
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
 * Returns a preview of staged data using the current column map and step pipeline,
 * but previews directly from the derived combined table using snake_case logical fields.
 */
async function getStagePreview({ customerId, ptrsId, limit = 50 }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const { rows: composed, headers } = await loadMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit,
      transaction: t,
    });

    // Apply row rules (if configured) for preview purposes
    let rowRules = null;
    try {
      const mapRow = await getColumnMap({
        customerId,
        ptrsId,
        transaction: t,
      });
      console.log("mapRow: ", mapRow);
      rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
      if (typeof rowRules === "string") {
        try {
          rowRules = JSON.parse(rowRules);
        } catch {
          rowRules = null;
        }
      }
      console.log("rowRules: ", rowRules);
    } catch (_) {
      rowRules = null;
    }

    const rulesResult = applyRules(
      composed,
      Array.isArray(rowRules) ? rowRules : []
    );
    console.log("rulesResult: ", rulesResult);
    const rowsAfterRules = rulesResult.rows || composed;
    console.log("rowsAfterRules: ", rowsAfterRules);
    const rulesStats = rulesResult.stats || null;

    await t.commit();
    console.log("headers: ", headers);
    console.log("rowsAfterRules: ", rowsAfterRules);
    console.log("rules: ", rulesStats);
    return {
      headers,
      rows: rowsAfterRules,
      stats: { rules: rulesStats },
    };
  } catch (err) {
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
