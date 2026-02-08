const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog } = require("./ptrs.service");

/**
 * Exclusions are eligibility decisions, not transformations.
 * Canonical pattern: SQL-first updates against tbl_ptrs_stage_row (jsonb),
 * never destroy/rebuild stage rows for exclusions.
 */

async function applyExclusionsAndPersist({
  customerId,
  ptrsId,
  profileId = null,
  category = "all",
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const started = Date.now();

  slog.info("PTRS v2 exclusions apply: starting", {
    action: "PtrsV2ExclusionsApplyStart",
    customerId,
    ptrsId,
    category,
  });

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const stats = { checksRun: 0, rowsExcluded: 0 };

    // Gov Entities (global ref table)
    if (category === "all" || category === "gov") {
      stats.checksRun += 1;

      const sql = `
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "data" = jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  s."data",
                  '{exclude}',
                  'true'::jsonb,
                  true
                ),
                '{exclude_from_metrics}',
                'true'::jsonb,
                true
              ),
              '{exclude_reason}',
              to_jsonb('GOV_ENTITY'::text),
              true
            ),
            '{exclude_comment}',
            to_jsonb(
              (
                'Government entity' ||
                CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
                CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
              )::text
            ),
            true
          ),
          "meta" = jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."meta",'{}'::jsonb),
                '{_stage}',
                to_jsonb('ptrs.v2.exclusionsApply'::text),
                true
              ),
              '{at}',
              to_jsonb(now()::text),
              true
            ),
            '{exclusions}',
            jsonb_build_object(
              'excluded', true,
              'reason', 'GOV_ENTITY',
              'comment',
                (
                  'Government entity' ||
                  CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
                  CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
                )::text
            ),
            true
          ),
          "updatedAt" = now()
        FROM "tbl_ptrs_gov_entity_ref" g
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND g."deletedAt" IS NULL
          AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
              = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
          AND COALESCE((s."data"->>'exclude')::boolean, false) = false
      `;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Stamp profileId onto meta for this run (only if provided), without touching data.
    if (profileId) {
      const profileSql = `
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "meta" = jsonb_set(
            COALESCE(s."meta",'{}'::jsonb),
            '{profileId}',
            to_jsonb(:profileId::text),
            true
          ),
          "updatedAt" = now()
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
      `;

      await sequelize.query(profileSql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });
    }

    await t.commit();

    const tookMs = Date.now() - started;

    slog.info("PTRS v2 exclusions apply: done", {
      action: "PtrsV2ExclusionsApplyDone",
      customerId,
      ptrsId,
      category,
      rowsExcluded: stats.rowsExcluded,
      tookMs,
    });

    return {
      // Backward compatible response shape for FE: persisted is "rows affected"
      persisted: stats.rowsExcluded,
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

async function previewExclusions({
  customerId,
  ptrsId,
  profileId = null, // accepted for API consistency (not required for gov)
  category = "all",
  limit = 10,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const effectiveLimit = Math.min(Number(limit) || 10, 50);

  const started = Date.now();

  slog.info("PTRS v2 exclusions preview: starting", {
    action: "PtrsV2ExclusionsPreviewStart",
    customerId,
    ptrsId,
    category,
    effectiveLimit,
  });

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const stats = { checksRun: 0, rowsExcluded: 0 };
    const result = {
      category,
      counts: {},
      alreadyExcludedCounts: {},
      samples: {},
    };

    if (category === "all" || category === "gov") {
      stats.checksRun += 1;

      const countSql = `
        SELECT
          COUNT(*)::int AS "matchedCount",
          SUM(CASE WHEN COALESCE((s."data"->>'exclude')::boolean, false) = true THEN 1 ELSE 0 END)::int AS "alreadyExcludedCount"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_gov_entity_ref" g
          ON regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
             = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND g."deletedAt" IS NULL
      `;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
      const alreadyExcluded =
        Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

      result.counts.gov = matched;
      result.alreadyExcludedCounts.gov = alreadyExcluded;

      const sampleSql = `
        SELECT
          (s."data"->>'row_no')::int AS "row_no",
          s."data"->>'payer_entity_abn' AS "payer_entity_abn",
          s."data"->>'payer_entity_name' AS "payer_entity_name",
          s."data"->>'payee_entity_abn' AS "payee_entity_abn",
          s."data"->>'payee_entity_name' AS "payee_entity_name",
          s."data"->>'invoice_reference_number' AS "invoice_reference_number",
          s."data"->>'payment_date' AS "payment_date",
          s."data"->>'payment_amount' AS "payment_amount",
          (
            'Government entity' ||
            CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
            CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
          )::text AS "exclude_comment",
          COALESCE((s."data"->>'exclude')::boolean, false) AS "alreadyExcluded"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_gov_entity_ref" g
          ON regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
             = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND g."deletedAt" IS NULL
        ORDER BY s."rowNo" ASC
        LIMIT :limit
      `;

      const [sampleRows] = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, limit: effectiveLimit },
        transaction: t,
      });

      result.samples.gov = Array.isArray(sampleRows) ? sampleRows : [];
    }

    await t.commit();

    const tookMs = Date.now() - started;

    slog.info("PTRS v2 exclusions preview: done", {
      action: "PtrsV2ExclusionsPreviewDone",
      customerId,
      ptrsId,
      category,
      tookMs,
    });

    return { tookMs, stats, result };
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
  previewExclusions,
};
