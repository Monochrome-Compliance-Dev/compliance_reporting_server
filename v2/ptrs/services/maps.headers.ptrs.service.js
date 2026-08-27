const db = require("@/db/database");
const { QueryTypes } = require("sequelize");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { logger } = require("@/helpers/logger");
const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const { getDatasetSample } = require("@/v2/ptrs/services/data.ptrs.service");

/**
 * Cheap header + example extraction for one explicitly selected transaction dataset.
 *
 * This is intentionally lightweight and should NOT touch PtrsImportRaw.
 * It prefers PtrsDataset.meta.headers and only falls back to a small dataset sample.
 */
async function getTransactionDatasetHeaderInfo({
  customerId,
  ptrsId,
  datasetId,
  limit = 3,
  offset = 0,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const selected = await db.PtrsDataset.findOne({
      where: { id: datasetId, customerId, ptrsId, purpose: "transaction" },
      attributes: ["id", "meta", "purpose"],
      raw: true,
      transaction: t,
    });
    if (!selected) {
      const error = new Error("Transaction dataset not found");
      error.statusCode = 404;
      throw error;
    }
    const meta =
      selected?.meta && typeof selected.meta === "object" ? selected.meta : {};

    let headers = Array.isArray(meta.headers) ? meta.headers : [];
    let examplesByHeader = {};

    try {
      const sample = datasetId
        ? await getDatasetSample({
            customerId,
            datasetId,
            limit,
            offset,
          })
        : null;

      if (sample) {
        if (!headers.length && Array.isArray(sample.headers)) {
          headers = sample.headers;
        }

        const rows = Array.isArray(sample.rows) ? sample.rows : [];
        for (const row of rows) {
          for (const [k, v] of Object.entries(row || {})) {
            if (examplesByHeader[k] != null) continue;
            if (v == null) continue;
            const s = String(v).trim();
            if (!s) continue;
            examplesByHeader[k] = v;
          }
        }
      }
    } catch (_) {
      // ignore sample failures; headers/examples are best-effort
    }

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return {
      datasetId: selected.id,
      headers: Array.isArray(headers) ? headers.map((h) => String(h)) : [],
      examplesByHeader,
    };
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getAllDatasetHeaderInfo({
  customerId,
  ptrsId,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      attributes: ["id", "role", "meta"],
      raw: true,
      transaction: t,
    });

    const headers = Array.from(
      new Set(
        (Array.isArray(dsRows) ? dsRows : []).flatMap((d) =>
          Array.isArray(d?.meta?.headers) ? d.meta.headers.map(String) : [],
        ),
      ),
    );

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return { headers };
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getImportSample({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  if (logger && logger.info) {
    slog.info("PTRS v2 getImportSample: begin dataset header/sample lookup", {
      action: "PtrsV2GetImportSample",
      customerId,
      ptrsId,
    });
  }

  const stack = new Error().stack?.split("\n").slice(1, 8).join("\n");

  slog.warn("PTRS v2 getImportSample: caller trace", {
    action: "PtrsV2GetImportSampleCallerTrace",
    customerId,
    ptrsId,
    stack,
  });

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.sequelize.query(
      `
      WITH dataset_base AS (
        SELECT
          d."id" AS "datasetId",
          d."role" AS "role",
          d."purpose" AS "purpose",
          d."sourceFormat" AS "sourceFormat",
          d."adapterType" AS "adapterType",
          d."referenceKind" AS "referenceKind",
          d."meta" AS "meta",
          d."createdAt" AS "createdAt",
          (d."purpose" = 'transaction') AS "isTransaction"
        FROM "tbl_ptrs_dataset" d
        WHERE d."customerId" = :customerId
          AND d."ptrsId" = :ptrsId
          AND d."deletedAt" IS NULL
      ),
      dataset_with_sample AS (
        SELECT
          b."datasetId",
          b."role",
          b."purpose",
          b."sourceFormat",
          b."adapterType",
          b."referenceKind",
          b."meta",
          b."createdAt",
          b."isTransaction",
          s."data" AS "sampleRow"
        FROM dataset_base b
        LEFT JOIN LATERAL (
          SELECT r."data"
          FROM "tbl_ptrs_import_raw" r
          WHERE r."customerId" = :customerId
            AND r."ptrsId" = :ptrsId
            AND r."datasetId" = b."datasetId"
          ORDER BY r."rowNo" ASC
          LIMIT 1
        ) s ON true
      )
      SELECT
        dws."datasetId",
        dws."role",
        dws."purpose",
        dws."sourceFormat",
        dws."adapterType",
        dws."referenceKind",
        dws."isTransaction",
        COALESCE(
          CASE
            WHEN jsonb_typeof(dws."meta"->'headers') = 'array'
              AND jsonb_array_length(dws."meta"->'headers') > 0
            THEN dws."meta"->'headers'
            ELSE NULL
          END,
          (
            SELECT COALESCE(jsonb_agg(k ORDER BY k), '[]'::jsonb)
            FROM jsonb_object_keys(COALESCE(dws."sampleRow", '{}'::jsonb)) AS k
          ),
          '[]'::jsonb
        ) AS "headers",
        COALESCE(dws."sampleRow", '{}'::jsonb) AS "sampleRow"
      FROM dataset_with_sample dws
      ORDER BY
        CASE WHEN dws."isTransaction" THEN 0 ELSE 1 END,
        lower(btrim(coalesce(dws."role", ''))) ASC,
        dws."createdAt" ASC,
        dws."datasetId" ASC
      `,
      {
        type: QueryTypes.SELECT,
        replacements: { customerId, ptrsId },
        transaction: t,
      },
    );

    if (logger && logger.info) {
      slog.info("PTRS v2 getImportSample: done dataset header/sample lookup", {
        action: "PtrsV2GetImportSample",
        customerId,
        ptrsId,
        datasetCount: Array.isArray(rows) ? rows.length : 0,
      });
    }

    await t.commit();

    return {
      datasets: Array.isArray(rows) ? rows : [],
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
  getImportSample,
  getTransactionDatasetHeaderInfo,
  getAllDatasetHeaderInfo,
};
