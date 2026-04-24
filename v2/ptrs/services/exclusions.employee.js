async function applyEmployeeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  profileId,
}) {
  if (!profileId) throw new Error("profileId is required for employee");

  const reason = "EMPLOYEE";

  const sql = `
    WITH matches AS (
      SELECT
        s."id" AS stage_id,
        MIN(r."name") AS matched_name
      FROM "tbl_ptrs_stage_row" s
      JOIN "tbl_ptrs_employee_ref" r
        ON r."customerId" = :customerId
       AND r."profileId" = :profileId
       AND r."deletedAt" IS NULL
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND (
          (
            s."payeeEntityAbn" IS NOT NULL
            AND r."abn" IS NOT NULL
            AND s."payeeEntityAbn" = r."abn"
          )
          OR
          (
            trim(COALESCE(s."payeeEntityName", '')) <> ''
            AND trim(COALESCE(r."name", '')) <> ''
            AND lower(s."payeeEntityName") LIKE '%' || lower(r."name") || '%'
          )
        )
        AND COALESCE(s."excludeReason", '') <> :reason
      GROUP BY s."id"
    )
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "excludedTradeCreditPayment" = true,
      "excludeReason" = :reason,
      "updatedAt" = now()
    FROM matches m
    WHERE s."id" = m.stage_id
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId, profileId, reason },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewEmployeeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  profileId,
  effectiveLimit,
}) {
  if (!profileId) throw new Error("profileId is required for employee");

  const reason = "EMPLOYEE";

  const matchedRowsCte = `
    WITH matches AS (
      SELECT
        s."id" AS stage_id,
        MIN(r."name") AS matched_name
      FROM "tbl_ptrs_stage_row" s
      JOIN "tbl_ptrs_employee_ref" r
        ON r."customerId" = :customerId
       AND r."profileId" = :profileId
       AND r."deletedAt" IS NULL
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND (
          (
            s."payeeEntityAbn" IS NOT NULL
            AND r."abn" IS NOT NULL
            AND s."payeeEntityAbn" = r."abn"
          )
          OR
          (
            trim(COALESCE(s."payeeEntityName", '')) <> ''
            AND trim(COALESCE(r."name", '')) <> ''
            AND lower(s."payeeEntityName") LIKE '%' || lower(r."name") || '%'
          )
        )
      GROUP BY s."id"
    ),
    matched_rows AS (
      SELECT
        s."id",
        s."rowNo",
        s."payerEntityAbn",
        s."payerEntityName",
        s."payeeEntityAbn",
        s."payeeEntityName",
        s."invoiceReferenceNumber",
        s."sourceAccountCode",
        s."paymentDate",
        s."paymentAmount",
        s."excludedTradeCreditPayment",
        s."excludeReason",
        (
          'Employee / payroll' ||
          CASE WHEN COALESCE(m."matched_name", '') <> '' THEN ' — ' || m."matched_name" ELSE '' END
        )::text AS "excludeComment"
      FROM "tbl_ptrs_stage_row" s
      JOIN matches m
        ON m.stage_id = s."id"
    )
  `;

  const countSql = `
    ${matchedRowsCte}
    SELECT
      COUNT(*)::int AS "matchedCount",
      COUNT(*) FILTER (
        WHERE COALESCE("excludeReason", '') = :reason
      )::int AS "alreadyExcludedCount"
    FROM matched_rows
  `;

  const [countRows] = await sequelize.query(countSql, {
    replacements: { customerId, ptrsId, profileId, reason },
    transaction,
  });

  const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
  const alreadyExcluded =
    Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    ${matchedRowsCte}
    SELECT
      "rowNo" AS "row_no",
      "payerEntityAbn" AS "payer_entity_abn",
      "payerEntityName" AS "payer_entity_name",
      "payeeEntityAbn" AS "payee_entity_abn",
      "payeeEntityName" AS "payee_entity_name",
      "invoiceReferenceNumber" AS "invoice_reference_number",
      "sourceAccountCode" AS "account_code",
      CASE
        WHEN "paymentDate" IS NOT NULL THEN "paymentDate"::text
        ELSE NULL
      END AS "payment_date",
      CASE
        WHEN "paymentAmount" IS NOT NULL THEN "paymentAmount"::text
        ELSE NULL
      END AS "payment_amount",
      "excludeComment" AS "exclude_comment",
      CASE
        WHEN COALESCE("excludeReason", '') = :reason THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM matched_rows
    ORDER BY "rowNo" ASC
    LIMIT :limit
  `;

  const [sampleRows] = await sequelize.query(sampleSql, {
    replacements: {
      customerId,
      ptrsId,
      profileId,
      reason,
      limit: effectiveLimit,
    },
    transaction,
  });

  return {
    matched,
    alreadyExcluded,
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyEmployeeExclusion,
  previewEmployeeExclusion,
};
