const db = require("@/db/database");
const { QueryTypes } = require("sequelize");

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

// --- Payment term mapping helpers ---
async function loadPaymentTermMap({ customerId, profileId, transaction }) {
  if (!customerId || !profileId) return new Map();

  // Use raw SQL so we're not coupled to a Sequelize model name/attribute mapping.
  // NOTE: quoted identifiers because Postgres will fold unquoted names to lower-case.
  const rows = await db.sequelize.query(
    `
    SELECT "raw", "transformedDays"
    FROM "tbl_ptrs_payment_term_map"
    WHERE "customerId" = :customerId
      AND "profileId" = :profileId
      AND "deletedAt" IS NULL
    `,
    {
      type: QueryTypes.SELECT,
      replacements: { customerId, profileId },
      transaction,
    }
  );

  const map = new Map();
  for (const r of rows || []) {
    const key = r?.raw != null ? String(r.raw).trim() : "";
    const val = Number(r?.transformedDays);
    if (key && Number.isFinite(val)) map.set(key, val);
  }
  return map;
}

function deriveTermCode(row) {
  if (!row || typeof row !== "object") return null;

  // Prefer the resolved/effective term code if present.
  const candidates = [
    row.invoice_payment_terms_effective,
    row.invoice_payment_terms_raw,
    row.payment_term,
    row.paymentTerm,
  ];

  for (const c of candidates) {
    if (c == null) continue;
    const s = String(c).trim();
    if (s) return s;
  }

  return null;
}

function inferTermDaysFromCode(code) {
  // Intentionally disabled.
  // We do NOT guess payment term days in staging. Term days must come from
  // tbl_ptrs_payment_term_map so data issues are visible and fixable.
  // If you want inference for one-off backfills, do it in SQL or an explicit job.
  return null;
}

async function seedMissingPaymentTermMapRows({
  customerId,
  profileId,
  mappings,
  transaction,
}) {
  if (!customerId || !profileId) return { inserted: 0 };
  if (!mappings || typeof mappings[Symbol.iterator] !== "function") {
    return { inserted: 0 };
  }

  let inserted = 0;

  // We avoid ON CONFLICT because your table may not have the required unique constraint.
  for (const [term, days] of mappings) {
    const t = term != null ? String(term).trim() : "";
    const d = Number(days);
    if (!t || !Number.isFinite(d)) continue;

    const result = await db.sequelize.query(
      `
      INSERT INTO "tbl_ptrs_payment_term_map" (
        "customerId",
        "profileId",
        "raw",
        "transformedDays",
        "note",
        "createdBy",
        "updatedBy",
        "createdAt",
        "updatedAt"
      )
      SELECT
        :customerId,
        :profileId,
        :raw,
        :transformedDays,
        :note,
        :createdBy,
        :updatedBy,
        NOW(),
        NOW()
      WHERE NOT EXISTS (
        SELECT 1
        FROM "tbl_ptrs_payment_term_map"
        WHERE "customerId" = :customerId
          AND "profileId" = :profileId
          AND "raw" = :raw
          AND "deletedAt" IS NULL
      );
      `,
      {
        type: QueryTypes.INSERT,
        replacements: {
          customerId,
          profileId,
          raw: t,
          transformedDays: d,
          note: "Seeded from PTRS staging",
          createdBy: "system_mvp_seed",
          updatedBy: "system_mvp_seed",
        },
        transaction,
      }
    );

    // Sequelize returns different shapes depending on dialect/versions; be defensive.
    // If the INSERT actually happened, Postgres will report rowCount=1.
    const rowCount =
      (Array.isArray(result) && result[1] && result[1].rowCount) ||
      (result && result.rowCount) ||
      0;

    if (Number(rowCount) > 0) inserted += 1;
  }

  return { inserted };
}

function applyPaymentTermDaysFromMap(rows, termMap) {
  const stats = {
    lookedUp: 0,
    filled: 0,
    missing: 0,
    unmapped: 0,
    unmappedTermsSample: [],
  };

  if (!Array.isArray(rows) || !termMap || typeof termMap.get !== "function") {
    return { rows, stats };
  }

  for (const r of rows) {
    if (!r || typeof r !== "object") continue;

    // Only fill if not already present.
    const existing = r.payment_term_days;
    const hasExisting = existing !== null && typeof existing !== "undefined";
    if (hasExisting) continue;

    const code = deriveTermCode(r);
    if (!code) {
      stats.missing += 1;
      if (!Array.isArray(r._stageErrors)) r._stageErrors = [];
      r._stageErrors.push({
        code: "PAYMENT_TERM_MISSING",
        message:
          "Payment term code is missing; cannot derive payment_term_days",
        field: "invoice_payment_terms_effective",
        value: null,
      });
      continue;
    }

    stats.lookedUp += 1;

    const mapped = termMap.get(code);
    if (Number.isFinite(mapped)) {
      r.payment_term_days = mapped;
      stats.filled += 1;
      continue;
    }

    // No guessing in staging. If it's not mapped, make it visible.
    stats.unmapped += 1;
    stats.missing += 1;

    if (!Array.isArray(r._stageErrors)) r._stageErrors = [];
    r._stageErrors.push({
      code: "PAYMENT_TERM_UNMAPPED",
      message: "Payment term code is not mapped in tbl_ptrs_payment_term_map",
      field: "invoice_payment_terms_effective",
      value: code,
    });

    if (stats.unmappedTermsSample.length < 10)
      stats.unmappedTermsSample.push(code);
  }

  return { rows, stats };
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

    // Canonical fields are already materialised on mapped rows before staging.
    // Staging must not re-project/guess fields.
    const rows = baseRows;

    slog.info("PTRS v2 stagePtrs: loaded mapped rows", {
      action: "PtrsV2StagePtrsLoadedMappedRows",
      customerId,
      ptrsId,
      rowsCount: Array.isArray(rows) ? rows.length : 0,
      sampleRowKeys: rows && rows[0] ? Object.keys(rows[0]) : null,
    });

    // 2) Apply row-level rules (if any) independently of preview
    let stagedRows = rows;
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
        stagedRows,
        Array.isArray(rowRules) ? rowRules : []
      );
      stagedRows = rulesResult.rows || stagedRows;
      rulesStats = rulesResult.stats || null;
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to apply row rules", {
        action: "PtrsV2StagePtrsApplyRules",
        customerId,
        ptrsId,
        error: err.message,
      });
    }

    // 2b) Derive payment_term_days from tbl_ptrs_payment_term_map (profile-scoped)
    // This keeps metrics simple: they just read `payment_term_days` from staged JSON.
    let paymentTermStats = null;
    try {
      if (profileId) {
        const termMap = await loadPaymentTermMap({
          customerId,
          profileId,
          transaction: t,
        });

        const termResult = applyPaymentTermDaysFromMap(stagedRows, termMap);
        stagedRows = termResult.rows;
        paymentTermStats = termResult.stats;

        if (
          paymentTermStats?.missing ||
          paymentTermStats?.filled ||
          paymentTermStats?.unmapped
        ) {
          slog.info("PTRS v2 stagePtrs: payment term mapping stats", {
            action: "PtrsV2StagePtrsPaymentTermMap",
            customerId,
            ptrsId,
            profileId,
            ...paymentTermStats,
          });
        }
      } else {
        slog.warn(
          "PTRS v2 stagePtrs: profileId not provided; skipping payment term mapping",
          {
            action: "PtrsV2StagePtrsPaymentTermMapSkipped",
            customerId,
            ptrsId,
          }
        );
      }
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to apply payment term mapping", {
        action: "PtrsV2StagePtrsPaymentTermMapFailed",
        customerId,
        ptrsId,
        profileId: profileId || null,
        error: err?.message,
      });
    }

    // 3) Persist into tbl_ptrs_stage_row if requested
    let persistedCount = null;
    if (persist) {
      const basePayload = stagedRows.map((r) => {
        const rowNoVal = Number(r?.row_no ?? r?.rowNo ?? 0) || 0;

        // Persist the full resolved row into JSONB `data`.
        // NOTE: tbl_ptrs_stage_row only has: customerId, ptrsId, rowNo, data, errors, meta (+ timestamps)
        const dataObjBase =
          r && typeof r === "object" && Object.keys(r).length
            ? r
            : { _warning: "⚠️ No mapped data for this row" };

        // If the rules engine excluded the row, persist canonical exclusion fields.
        const shouldExclude = !!r?.exclude;
        const excludeComment =
          Array.isArray(r?._warnings) && r._warnings.length
            ? String(r._warnings[0])
            : shouldExclude
              ? "Excluded by rule"
              : null;

        const dataObj = shouldExclude
          ? {
              ...dataObjBase,
              exclude_from_metrics: true,
              exclude_comment: excludeComment,
              exclude_set_at: new Date().toISOString(),
              exclude_set_by: userId || null,
            }
          : dataObjBase;

        return {
          customerId: String(customerId),
          ptrsId: String(ptrsId),
          rowNo: rowNoVal,
          data: dataObj,
          errors:
            Array.isArray(r?._stageErrors) && r._stageErrors.length
              ? r._stageErrors
              : null,
          meta: {
            _stage: "ptrs.v2.stagePtrs",
            at: new Date().toISOString(),
            rules: {
              applied: Array.isArray(r?._appliedRules) ? r._appliedRules : [],
              exclude: !!r?.exclude,
              exclude_comment: dataObj?.exclude_comment || null,
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

      // IMPORTANT: staging must be idempotent.
      // On persist runs, we soft-delete any existing *active* stage rows for this ptrs
      // before inserting the new snapshot. We do this with raw SQL so we are not
      // dependent on Sequelize's destroy/paranoid behaviour (and to avoid silent no-ops).
      try {
        const [_, meta] = await db.sequelize.query(
          `
          UPDATE "tbl_ptrs_stage_row"
          SET "deletedAt" = NOW(), "updatedAt" = NOW()
          WHERE "customerId" = :customerId
            AND "ptrsId" = :ptrsId
            AND "deletedAt" IS NULL
          `,
          {
            type: QueryTypes.UPDATE,
            replacements: { customerId, ptrsId },
            transaction: t,
          }
        );

        // `meta` shape varies by Sequelize version; log defensively.
        const clearedCount =
          (meta && typeof meta.rowCount === "number" && meta.rowCount) ||
          (meta &&
            typeof meta.affectedRows === "number" &&
            meta.affectedRows) ||
          0;

        slog.info("PTRS v2 stagePtrs: cleared active stage rows", {
          action: "PtrsV2StagePtrsClearActive",
          customerId,
          ptrsId,
          clearedCount,
        });
      } catch (e) {
        // If this fails, do NOT proceed to insert (otherwise we will duplicate rows).
        slog.error("PTRS v2 stagePtrs: failed to clear active stage rows", {
          action: "PtrsV2StagePtrsClearActiveFailed",
          customerId,
          ptrsId,
          error: e?.message,
        });
        throw e;
      }

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

      // With paranoid enabled, this count will automatically exclude soft-deleted rows.
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
          rowsIn: stagedRows.length,
          rowsOut: stagedRows.length,
          stats: { rules: rulesStats, paymentTerms: paymentTermStats },
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
      rowsIn: stagedRows.length,
      rowsOut: stagedRows.length,
      persistedCount,
      tookMs,
      sample: stagedRows[0] || null,
      stats: { rules: rulesStats, paymentTerms: paymentTermStats },
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
