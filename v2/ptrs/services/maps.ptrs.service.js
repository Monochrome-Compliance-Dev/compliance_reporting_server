const db = require("@/db/database");

const { logger } = require("@/helpers/logger");
const {
  safeMeta,
  slog,
  toSnake,
  createExecutionRun,
  updateExecutionRun,
} = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const { QueryTypes } = require("sequelize");

const { createPtrsTrace, hrMsSince } = require("@/helpers/ptrsTrackerLog");

const {
  getMapStaleness,
} = require("@/v2/ptrs/services/maps.staleness.ptrs.service");

const {
  composeMappedRowsForPtrs,
} = require("@/v2/ptrs/services/maps.compose.ptrs.service");

const {
  persistMappedRowsInBatches,
  buildMappedDatasetForPtrs: buildMappedDatasetForPtrsImpl,
} = require("@/v2/ptrs/services/maps.build.ptrs.service");

module.exports = {
  buildMappedDatasetForPtrs,
  composeMappedRowsForPtrs,
  loadMappedRowsForPtrs,
  getMapCompletionGate,
};

const {
  REQUIRED_CANONICAL_FIELDS,
} = require("@/v2/ptrs/services/maps.config.ptrs.service");

function parseDateFlexible(value) {
  if (value == null) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;

  const s = String(value).trim();
  if (!s) return null;

  // dd/mm/yyyy
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s);
  if (m) {
    const dd = Number(m[1]);
    const mm = Number(m[2]);
    const yyyy = Number(m[3]);
    if (!Number.isFinite(dd) || !Number.isFinite(mm) || !Number.isFinite(yyyy))
      return null;
    const d = new Date(Date.UTC(yyyy, mm - 1, dd));
    if (
      d.getUTCFullYear() !== yyyy ||
      d.getUTCMonth() !== mm - 1 ||
      d.getUTCDate() !== dd
    )
      return null;
    return d;
  }

  // ISO-ish
  const iso = new Date(s);
  if (!Number.isNaN(iso.getTime())) return iso;

  return null;
}

// IMPORTANT:
// Map should materialise a combined source-rich, canonical-aware row only.
// It must NOT derive Stage/report convenience values, and it must NOT force
// full Stage shape by padding missing canonical fields with nulls.
// Stage owns canonical projection and downstream derivations.

function ensureCanonicalRowShape(row) {
  // Map remains canonical-aware, but it should not force full Stage shape.
  // Return only the fields that actually exist on the mapped row.
  return { ...(row || {}) };
}

async function getMappedDataCompletenessGate({
  customerId,
  ptrsId,
  profileId = null,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const mappedFieldRows =
      profileId && db.PtrsFieldMap
        ? await db.PtrsFieldMap.findAll({
            where: { customerId, ptrsId, profileId },
            attributes: ["canonicalField"],
            raw: true,
            transaction: t,
          })
        : [];

    const mappedCanonicalFields = Array.from(
      new Set(
        (mappedFieldRows || [])
          .map((r) => toSnake(r?.canonicalField))
          .filter(Boolean),
      ),
    );

    const requiredFields = Array.from(new Set(REQUIRED_CANONICAL_FIELDS));
    const missingRequiredMappings = requiredFields.filter(
      (field) => !mappedCanonicalFields.includes(field),
    );

    const fieldsToAssess =
      requiredFields.length > 0 ? requiredFields : mappedCanonicalFields;

    const rowCountRows = await db.sequelize.query(
      `
      SELECT COUNT(1)::int AS "rowCount"
      FROM "tbl_ptrs_mapped_row"
      WHERE "customerId" = :customerId
        AND "ptrsId" = :ptrsId
      `,
      {
        type: QueryTypes.SELECT,
        replacements: { customerId, ptrsId },
        transaction: t,
      },
    );

    const rowCount = Number(rowCountRows?.[0]?.rowCount) || 0;

    let fieldResults = [];

    if (fieldsToAssess.length > 0) {
      const valuesSql = fieldsToAssess
        .map((field) => `('${String(field).replace(/'/g, "''")}')`)
        .join(",\n          ");

      fieldResults = await db.sequelize.query(
        `
        WITH fields("canonicalField") AS (
          VALUES
          ${valuesSql}
        ),
        row_scope AS (
          SELECT "rowNo", "data"
          FROM "tbl_ptrs_mapped_row"
          WHERE "customerId" = :customerId
            AND "ptrsId" = :ptrsId
        ),
        row_count_cte AS (
          SELECT COUNT(1)::int AS "rowCount"
          FROM row_scope
        )
        SELECT
          f."canonicalField",
          rc."rowCount",
          COUNT(rs.*) FILTER (
            WHERE NULLIF(BTRIM(COALESCE(rs."data"->>f."canonicalField", '')), '') IS NOT NULL
          )::int AS "populatedCount",
          COUNT(rs.*) FILTER (
            WHERE NULLIF(BTRIM(COALESCE(rs."data"->>f."canonicalField", '')), '') IS NULL
          )::int AS "missingCount",
          CASE
            WHEN rc."rowCount" = 0 THEN 0
            ELSE ROUND(
              (
                COUNT(rs.*) FILTER (
                  WHERE NULLIF(BTRIM(COALESCE(rs."data"->>f."canonicalField", '')), '') IS NOT NULL
                )::numeric * 100.0
              ) / rc."rowCount"::numeric,
              2
            )
          END AS "completenessPct",
          COALESCE(
            (
              array_agg(rs."rowNo" ORDER BY rs."rowNo") FILTER (
                WHERE NULLIF(BTRIM(COALESCE(rs."data"->>f."canonicalField", '')), '') IS NULL
              )
            )[1:5],
            ARRAY[]::integer[]
          ) AS "sampleMissingRowNos"
        FROM fields f
        CROSS JOIN row_count_cte rc
        LEFT JOIN row_scope rs ON TRUE
        GROUP BY f."canonicalField", rc."rowCount"
        ORDER BY f."canonicalField" ASC
        `,
        {
          type: QueryTypes.SELECT,
          replacements: { customerId, ptrsId },
          transaction: t,
        },
      );
    }

    const fields = (fieldResults || []).map((row) => ({
      canonicalField: row.canonicalField,
      rowCount: Number(row.rowCount) || 0,
      populatedCount: Number(row.populatedCount) || 0,
      missingCount: Number(row.missingCount) || 0,
      completenessPct: Number(row.completenessPct) || 0,
      sampleMissingRowNos: Array.isArray(row.sampleMissingRowNos)
        ? row.sampleMissingRowNos.map((n) => Number(n)).filter(Number.isFinite)
        : [],
    }));

    const fieldsWithGaps = fields.filter((f) => Number(f.missingCount) > 0);
    const emptyFields = fields.filter(
      (f) => Number(f.rowCount) > 0 && Number(f.populatedCount) === 0,
    );

    const gate = {
      passed:
        missingRequiredMappings.length === 0 && fieldsWithGaps.length === 0,
      severity:
        missingRequiredMappings.length > 0
          ? "error"
          : fieldsWithGaps.length > 0
            ? "warning"
            : "success",
      summary: {
        rowCount,
        requiredFieldsCount: requiredFields.length,
        assessedFieldsCount: fields.length,
        missingRequiredMappingsCount: missingRequiredMappings.length,
        fieldsWithGapsCount: fieldsWithGaps.length,
        emptyFieldsCount: emptyFields.length,
      },
      missingRequiredMappings,
      fields,
    };

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return gate;
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getMapCompletionGate({
  customerId,
  ptrsId,
  profileId = null,
  transaction = null,
}) {
  return getMappedDataCompletenessGate({
    customerId,
    ptrsId,
    profileId,
    transaction,
  });
}

// Postgres JSONB will reject strings containing NUL (\u0000) bytes.
// Also, JSON cannot represent `undefined` values.

async function loadMappedRowsForPtrs({
  customerId,
  ptrsId,
  limit = 50,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const findOpts = {
    where: { customerId, ptrsId },
    order: [["rowNo", "ASC"]],
    attributes: ["rowNo", "data"],
    raw: true,
    transaction,
  };

  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    findOpts.limit = numericLimit;
  }

  const rows = await db.PtrsMappedRow.findAll(findOpts);

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 loadMappedRowsForPtrs: loaded mapped rows",
      safeMeta({
        customerId,
        ptrsId,
        requestedLimit: limit,
        rowsCount: Array.isArray(rows) ? rows.length : 0,
      }),
    );
  }

  const composed = rows.map((r) => {
    let base = r.data || {};

    // If data was accidentally stored as a JSON string, try to parse it defensively
    if (typeof base === "string") {
      try {
        const parsed = JSON.parse(base);
        if (parsed && typeof parsed === "object") {
          base = parsed;
        }
      } catch (_) {
        // leave base as-is if parsing fails
      }
    }

    // --- Normalise keys to snake_case to avoid camelCase + snake_case duplicates ---
    const normalised = {};
    for (const [k, v] of Object.entries(base || {})) {
      const key = toSnake(k);
      if (!key) continue;
      normalised[key] = v;
    }

    // ensure row_no is present for downstream logic
    normalised.row_no = r.rowNo;

    return ensureCanonicalRowShape(normalised);
  });

  // Simple header inference from the mapped rows
  const headers = Array.from(
    new Set(composed.flatMap((row) => Object.keys(row))),
  );

  if (logger && logger.debug && composed.length) {
    slog.debug(
      "PTRS v2 loadMappedRowsForPtrs: sample composed row",
      safeMeta({
        customerId,
        ptrsId,
        sampleRowKeys: Object.keys(composed[0] || {}),
        headersCount: headers.length,
      }),
    );
  }

  return { rows: composed, headers };
}

// Helper to persist mapped rows in batches for a ptrs run

// Build and persist the mapped + joined dataset for a ptrs run into PtrsMappedRow
async function buildMappedDatasetForPtrs({
  customerId,
  ptrsId,
  actorId = null,
}) {
  return buildMappedDatasetForPtrsImpl({
    customerId,
    ptrsId,
    actorId,
    beginTransactionWithCustomerContext,
    createPtrsTrace,
    hrMsSince,
    safeMeta,
    slog,
    getMapStaleness,
    createExecutionRun,
    updateExecutionRun,
    getMapCompletionGate,
    persistMappedRowsInBatches,
    composeMappedRowsForPtrs: (args) =>
      composeMappedRowsForPtrs({
        ...args,
        hrMsSince,
        parseDateFlexible,
      }),
    ensureCanonicalRowShape,
    db,
  });
}
