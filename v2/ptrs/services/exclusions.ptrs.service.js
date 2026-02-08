const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog, safeMeta } = require("./ptrs.service");
const { loadMappedRowsForPtrs } = require("./tablesAndMaps.ptrs.service");

/**
 * Apply deterministic TCP exclusion checks to mapped rows.
 * Exclusions are eligibility decisions, not transformations.
 */
async function applyExclusionsAndPersist({
  customerId,
  ptrsId,
  profileId = null,
  limit = null, // null = full dataset
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const effectiveLimit =
    limit == null || typeof limit === "undefined"
      ? null
      : Math.min(Number(limit) || 50, 5000);

  const started = Date.now();

  slog.info("PTRS v2 exclusions apply: starting", {
    action: "PtrsV2ExclusionsApplyStart",
    customerId,
    ptrsId,
    effectiveLimit,
  });

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // 1) Load mapped rows (same source as Rules)
    const { rows: baseRows } = await loadMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: effectiveLimit,
      transaction: t,
    });

    slog.info("PTRS v2 exclusions apply: composed base rows", {
      action: "PtrsV2ExclusionsApplyComposed",
      customerId,
      ptrsId,
      baseRowCount: Array.isArray(baseRows) ? baseRows.length : 0,
    });

    // 2) Apply exclusions (MVP: placeholder runner)
    // Each exclusion marks:
    //   r.exclude = true
    //   r.exclude_from_metrics = true
    //   r.exclude_comment = <human readable reason>
    const stats = {
      checksRun: 0,
      rowsExcluded: 0,
    };

    const rows = Array.isArray(baseRows) ? baseRows : [];

    for (const r of rows) {
      // Placeholder – real checks will be added incrementally
      // Intentionally empty to establish pipeline + persistence shape
    }

    // 3) Persist back to tbl_ptrs_stage_row (mirrors Rules persist logic)
    const payload = rows.map((r) => {
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
          _stage: "ptrs.v2.exclusionsApply",
          at: new Date().toISOString(),
          profileId: profileId || null,
          exclusions: {
            excluded: !!r.exclude,
            comment: r.exclude_comment || null,
          },
        },
      };
    });

    await db.PtrsStageRow.destroy({
      where: { customerId, ptrsId },
      transaction: t,
    });

    if (payload.length) {
      await db.PtrsStageRow.bulkCreate(payload, {
        validate: false,
        returning: false,
        transaction: t,
      });
    }

    await t.commit();

    const tookMs = Date.now() - started;

    slog.info("PTRS v2 exclusions apply: done", {
      action: "PtrsV2ExclusionsApplyDone",
      customerId,
      ptrsId,
      persisted: payload.length,
      tookMs,
    });

    return {
      persisted: payload.length,
      tookMs,
      stats,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

module.exports = {
  applyExclusionsAndPersist,
};
