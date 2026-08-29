const crypto = require("crypto");

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  appendTransformationHistorySql,
} = require("./stage.transformation-history");

module.exports = {
  importResults,
  getStatus,
  exportAbnCsv,
  validateAppliedSbi,
  reapplyLatestResults,
  buildSbiValidationSql,
  buildSbiExportSql,
  buildSbiReapplySql,
  buildSbiReapplyStatsSql,
};
async function getLatestAppliedUpload({ customerId, ptrsId, transaction }) {
  // Prefer most recent upload with a usable status
  return db.PtrsSbiUpload.findOne({
    where: {
      customerId,
      ptrsId,
      status: ["APPLIED", "APPLIED_WITH_WARNINGS"],
    },
    order: [["createdAt", "DESC"]],
    attributes: ["id", "status"],
    raw: true,
    transaction,
  });
}

const SBI_VALIDATION_SAMPLE_LIMIT = 200;

function buildSbiValidationSql() {
  return `
    WITH stage AS MATERIALIZED (
      SELECT
        stage_row."id",
        stage_row."rowNo",
        (
          COALESCE(
            stage_row."meta"->'rules'->'exclude',
            'false'::jsonb
          ) = 'true'::jsonb
          OR COALESCE(
            stage_row."data"->'exclude',
            'false'::jsonb
          ) = 'true'::jsonb
          OR COALESCE(
            stage_row."data"->'exclude_from_metrics',
            'false'::jsonb
          ) = 'true'::jsonb
        ) AS excluded,
        NULLIF(
          regexp_replace(
            COALESCE(stage_row."data"->>'payee_entity_abn', ''),
            '\\D',
            '',
            'g'
          ),
          ''
        ) AS abn,
        CASE
          WHEN jsonb_typeof(
            stage_row."data"->'is_small_business'
          ) = 'boolean'
            THEN (stage_row."data"->>'is_small_business')::boolean
          ELSE NULL
        END AS actual,
        stage_row."data"->>'small_business_evidence_id' AS evidence_id
      FROM "tbl_ptrs_stage_row" stage_row
      WHERE stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
    ),
    joined AS MATERIALIZED (
      SELECT
        stage.*,
        result."id" AS result_id,
        result."outcome",
        result."isValidAbn" AS result_is_valid,
        CASE
          WHEN result."outcome" = :smallOutcome THEN true
          WHEN result."outcome" = :notSmallOutcome THEN false
          ELSE NULL
        END AS expected
      FROM stage
      LEFT JOIN "tbl_ptrs_sbi_result" result
        ON result."customerId" = :customerId
       AND result."ptrsId" = :ptrsId
       AND result."sbiUploadId" = :uploadId
       AND result."deletedAt" IS NULL
       AND result."abn" = stage.abn
    ),
    classified AS MATERIALIZED (
      SELECT
        joined.*,
        CASE
          WHEN joined.excluded THEN NULL
          WHEN joined.abn IS NULL THEN 'PAYEE_ABN_MISSING'
          WHEN joined.abn !~ '^\\d{11}$' THEN 'PAYEE_ABN_INVALID'
          WHEN joined.result_id IS NOT NULL
            AND (
              joined.result_is_valid IS FALSE
              OR joined.outcome ~* 'not recognised as a valid abn'
            ) THEN 'SBI_INVALID_ABN'
          WHEN joined.result_id IS NULL THEN 'SBI_NO_MATCH'
          WHEN joined.expected IS NULL THEN 'SBI_UNKNOWN_OUTCOME'
          WHEN joined.evidence_id IS DISTINCT FROM :uploadId
            THEN 'SBI_EVIDENCE_MISSING'
          WHEN joined.actual IS DISTINCT FROM joined.expected
            THEN 'SBI_FLAG_MISMATCH'
          ELSE NULL
        END AS issue_code
      FROM joined
    ),
    counts AS (
      SELECT
        COUNT(*)::int AS "totalRows",
        COUNT(*) FILTER (WHERE classified.excluded)::int AS "excludedRows",
        COUNT(*) FILTER (
          WHERE classified.issue_code = 'PAYEE_ABN_MISSING'
        )::int AS "missingPayeeAbnCount",
        COUNT(*) FILTER (
          WHERE classified.issue_code = 'PAYEE_ABN_INVALID'
        )::int AS "invalidPayeeAbnCount",
        COUNT(*) FILTER (
          WHERE classified.issue_code = 'SBI_NO_MATCH'
        )::int AS "abnMissingFromSbiResultsCount",
        COUNT(*) FILTER (
          WHERE classified.issue_code = 'SBI_EVIDENCE_MISSING'
        )::int AS "sbiEvidenceMismatchCount",
        COUNT(*) FILTER (
          WHERE classified.issue_code = 'SBI_FLAG_MISMATCH'
        )::int AS "sbiOutcomeMismatchCount",
        COUNT(*) FILTER (
          WHERE classified.issue_code IN (
            'PAYEE_ABN_MISSING',
            'PAYEE_ABN_INVALID',
            'SBI_INVALID_ABN',
            'SBI_UNKNOWN_OUTCOME',
            'SBI_EVIDENCE_MISSING',
            'SBI_FLAG_MISMATCH'
          )
        )::int AS "blockerCount",
        COUNT(*) FILTER (
          WHERE classified.issue_code = 'SBI_NO_MATCH'
        )::int AS "warningCount",
        (
          SELECT COUNT(*)::int
          FROM "tbl_ptrs_sbi_result" result
          WHERE result."customerId" = :customerId
            AND result."ptrsId" = :ptrsId
            AND result."sbiUploadId" = :uploadId
            AND result."deletedAt" IS NULL
        ) AS "totalResults"
      FROM classified
    ),
    ranked_issues AS (
      SELECT
        classified.*,
        CASE
          WHEN classified.issue_code = 'SBI_NO_MATCH' THEN 'warning'
          ELSE 'blocker'
        END AS issue_type,
        row_number() OVER (
          PARTITION BY CASE
            WHEN classified.issue_code = 'SBI_NO_MATCH' THEN 'warning'
            ELSE 'blocker'
          END
          ORDER BY classified."rowNo", classified."id"
        ) AS sample_rank
      FROM classified
      WHERE classified.issue_code IS NOT NULL
    ),
    sample_issues AS (
      SELECT
        ranked_issues.issue_type,
        ranked_issues.sample_rank,
        jsonb_build_object(
          'stageRowId', ranked_issues."id",
          'rowNo', ranked_issues."rowNo",
          'code', ranked_issues.issue_code,
          'message', CASE ranked_issues.issue_code
            WHEN 'PAYEE_ABN_MISSING' THEN 'Missing payee_entity_abn'
            WHEN 'PAYEE_ABN_INVALID'
              THEN 'payee_entity_abn is not a valid 11-digit ABN'
            WHEN 'SBI_INVALID_ABN'
              THEN 'SBI results indicate this ABN is invalid/unrecognised'
            WHEN 'SBI_NO_MATCH'
              THEN 'No SBI outcome found for this payee ABN (possible mismatched SBI file)'
            WHEN 'SBI_UNKNOWN_OUTCOME'
              THEN 'SBI outcome is not recognised'
            WHEN 'SBI_EVIDENCE_MISSING'
              THEN 'Row is missing the expected small business evidence id for the latest SBI upload'
            WHEN 'SBI_FLAG_MISMATCH'
              THEN 'Row small business flag does not match the SBI outcome'
          END,
          'payeeAbn', COALESCE(ranked_issues.abn, '')
        ) || CASE ranked_issues.issue_code
          WHEN 'SBI_INVALID_ABN' THEN jsonb_build_object(
            'outcome', ranked_issues.outcome
          )
          WHEN 'SBI_UNKNOWN_OUTCOME' THEN jsonb_build_object(
            'outcome', ranked_issues.outcome
          )
          WHEN 'SBI_EVIDENCE_MISSING' THEN jsonb_build_object(
            'expectedEvidenceId', :uploadId,
            'actualEvidenceId', ranked_issues.evidence_id
          )
          WHEN 'SBI_FLAG_MISMATCH' THEN jsonb_build_object(
            'outcome', ranked_issues.outcome,
            'expected', ranked_issues.expected,
            'actual', ranked_issues.actual
          )
          ELSE '{}'::jsonb
        END AS issue
      FROM ranked_issues
      WHERE ranked_issues.sample_rank <= :sampleLimit
    ),
    samples AS (
      SELECT
        COALESCE(
          jsonb_agg(sample_issues.issue ORDER BY sample_issues.sample_rank)
            FILTER (WHERE sample_issues.issue_type = 'blocker'),
          '[]'::jsonb
        ) AS blockers,
        COALESCE(
          jsonb_agg(sample_issues.issue ORDER BY sample_issues.sample_rank)
            FILTER (WHERE sample_issues.issue_type = 'warning'),
          '[]'::jsonb
        ) AS warnings
      FROM sample_issues
    )
    SELECT counts.*, samples.blockers, samples.warnings
    FROM counts
    CROSS JOIN samples
  `;
}

/**
 * Validates that the latest SBI upload was applied consistently to stage rows.
 * This is intentionally SBI-scoped (not overall report validation).
 */
async function validateAppliedSbi({ customerId, ptrsId, userId = null, mode }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      attributes: ["id"],
      raw: true,
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    const latestSbi = await getLatestAppliedUpload({
      customerId,
      ptrsId,
      transaction: t,
    });

    if (!latestSbi) {
      const blockers = [
        {
          code: "SBI_MISSING",
          message:
            "SBI Check has not been applied for this PTRS run. Upload SBI results before validating.",
        },
      ];
      await t.commit();

      return {
        status: "BLOCKED",
        ptrsId,
        mode,
        sbi: { required: true, latestUploadId: null },
        counts: {
          totalRows: 0,
          excludedRows: 0,
          blockers: blockers.length,
          warnings: 0,
        },
        blockers,
        warnings: [],
      };
    }

    const validationRows = await db.sequelize.query(buildSbiValidationSql(), {
      replacements: {
        customerId,
        ptrsId,
        uploadId: latestSbi.id,
        smallOutcome: OUTCOME_SMALL,
        notSmallOutcome: OUTCOME_NOT_SMALL,
        sampleLimit: SBI_VALIDATION_SAMPLE_LIMIT,
      },
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    const validation = validationRows?.[0] || {};
    const blockers = Array.isArray(validation.blockers)
      ? validation.blockers
      : [];
    const warnings = Array.isArray(validation.warnings)
      ? validation.warnings
      : [];
    const blockerCount = Number(validation.blockerCount) || 0;
    const warningCount = Number(validation.warningCount) || 0;
    const status =
      blockerCount > 0
        ? "BLOCKED"
        : warningCount > 0
          ? "PASSED_WITH_WARNINGS"
          : "PASSED";

    await t.commit();

    return {
      status,
      ptrsId,
      mode,
      sbi: {
        required: true,
        latestUploadId: latestSbi.id,
        uploadStatus: latestSbi.status,
        totalResults: Number(validation.totalResults) || 0,
      },
      counts: {
        totalRows: Number(validation.totalRows) || 0,
        excludedRows: Number(validation.excludedRows) || 0,
        blockers: blockerCount,
        warnings: warningCount,
        missingPayeeAbnCount: Number(validation.missingPayeeAbnCount) || 0,
        invalidPayeeAbnCount: Number(validation.invalidPayeeAbnCount) || 0,
        abnMissingFromSbiResultsCount:
          Number(validation.abnMissingFromSbiResultsCount) || 0,
        sbiEvidenceMismatchCount:
          Number(validation.sbiEvidenceMismatchCount) || 0,
        sbiOutcomeMismatchCount:
          Number(validation.sbiOutcomeMismatchCount) || 0,
      },
      blockers,
      warnings,
    };
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}

const OUTCOME_SMALL = "Small business for payment times reporting";
const OUTCOME_NOT_SMALL = "Not a small business for payment times reporting";

function normalizeAbn(value) {
  if (value == null) return "";
  return String(value).replace(/\D+/g, "");
}

function buildSbiReapplyStatsSql() {
  const excludedSql = `(
    COALESCE(s."meta"->'rules'->'exclude', 'false'::jsonb) = 'true'::jsonb
    OR COALESCE(s."data"->'exclude', 'false'::jsonb) = 'true'::jsonb
    OR COALESCE(s."data"->'exclude_from_metrics', 'false'::jsonb) = 'true'::jsonb
  )`;
  const abnSql = `NULLIF(
    regexp_replace(COALESCE(s."data"->>'payee_entity_abn', ''), '\\D', '', 'g'),
    ''
  )`;
  return `
    WITH stage AS MATERIALIZED (
      SELECT
        s."id",
        ${excludedSql} AS excluded,
        ${abnSql} AS abn,
        s."data"->'is_small_business' AS current_value,
        s."data"->>'small_business_evidence_id' AS current_evidence,
        s."data"->>'small_business_source' AS current_source,
        s."data"->>'small_business_outcome' AS current_outcome
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
    ),
    existing_changes AS MATERIALIZED (
      SELECT DISTINCT change."paymentRowId"
      FROM "tbl_ptrs_sbi_row_change" change
      WHERE change."customerId" = :customerId
        AND change."ptrsId" = :ptrsId
        AND change."sbiUploadId" = :uploadId
        AND change."deletedAt" IS NULL
    ),
    joined AS MATERIALIZED (
      SELECT
        stage.*,
        result."id" AS result_id,
        result."isValidAbn" AS result_is_valid,
        result."outcome",
        CASE
          WHEN result."outcome" = :smallOutcome THEN true
          WHEN result."outcome" = :notSmallOutcome THEN false
          ELSE NULL
        END AS expected,
        existing_changes."paymentRowId" IS NOT NULL
          AS change_record_exists
      FROM stage
      LEFT JOIN "tbl_ptrs_sbi_result" result
        ON result."customerId" = :customerId
       AND result."ptrsId" = :ptrsId
       AND result."sbiUploadId" = :uploadId
       AND result."deletedAt" IS NULL
       AND result."abn" = stage.abn
      LEFT JOIN existing_changes
        ON existing_changes."paymentRowId" = stage."id"
    )
    SELECT
      COUNT(*)::int AS "totalRows",
      COUNT(*) FILTER (WHERE joined.excluded)::int AS "excludedRows",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded AND joined.abn IS NULL
      )::int AS "missingAbnRows",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded AND joined.abn IS NOT NULL
      )::int AS "rowsWithPayeeAbn",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded
          AND joined.abn ~ '^\\d{11}$'
          AND joined.result_id IS NOT NULL
      )::int AS "matchedAbns",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded
          AND joined.abn ~ '^\\d{11}$'
          AND joined.result_id IS NOT NULL
          AND joined.result_is_valid IS FALSE
      )::int AS "invalidMatchRows",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded
          AND joined.abn ~ '^\\d{11}$'
          AND joined.result_id IS NOT NULL
          AND joined.result_is_valid IS TRUE
          AND joined.outcome NOT IN (:smallOutcome, :notSmallOutcome)
      )::int AS "unknownOutcomeRows",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded
          AND joined.abn ~ '^\\d{11}$'
          AND joined.result_is_valid IS TRUE
          AND joined.expected IS NOT NULL
          AND (
            joined.current_value IS DISTINCT FROM to_jsonb(joined.expected)
            OR joined.current_evidence IS DISTINCT FROM :uploadId
            OR joined.current_source IS DISTINCT FROM 'SBI_UPLOAD'
            OR joined.current_outcome IS DISTINCT FROM joined.outcome
          )
      )::int AS "dataChangeRows",
      COUNT(*) FILTER (
        WHERE NOT joined.excluded
          AND joined.abn ~ '^\\d{11}$'
          AND joined.result_is_valid IS TRUE
          AND joined.expected IS NOT NULL
          AND NOT joined.change_record_exists
      )::int AS "historyCheckRows"
    FROM joined
  `;
}

function buildSbiReapplySql() {
  const historyEventSql = `jsonb_build_object(
    'key', candidate.history_key,
    'kind', 'sbi',
    'comment', 'SBI status resolved ' || candidate.expected::text
      || ' from SBI upload ' || :uploadId
      || ' for payee ABN ' || candidate.abn,
    'sourceStageRowIds', jsonb_build_array(candidate."id"),
    'targetStageRowIds', jsonb_build_array(candidate."id"),
    'details', jsonb_build_object(
      'payeeAbn', candidate.abn,
      'outcome', candidate.outcome,
      'isSmallBusiness', candidate.expected,
      'source', 'SBI_UPLOAD',
      'evidenceId', :uploadId
    )
  )`;
  const nextMetaSql = appendTransformationHistorySql(
    'stage_row."meta"',
    historyEventSql,
  );

  return `
    WITH existing_changes AS MATERIALIZED (
      SELECT DISTINCT change."paymentRowId"
      FROM "tbl_ptrs_sbi_row_change" change
      WHERE change."customerId" = :customerId
        AND change."ptrsId" = :ptrsId
        AND change."sbiUploadId" = :uploadId
        AND change."deletedAt" IS NULL
    ),
    stage_source AS MATERIALIZED (
      SELECT
        stage_row."id",
        stage_row."rowNo",
        NULLIF(
          regexp_replace(
            COALESCE(stage_row."data"->>'payee_entity_abn', ''),
            '\\D',
            '',
            'g'
          ),
          ''
        ) AS abn,
        (
          COALESCE(
            stage_row."meta"->'rules'->'exclude',
            'false'::jsonb
          ) = 'true'::jsonb
          OR COALESCE(stage_row."data"->'exclude', 'false'::jsonb)
            = 'true'::jsonb
          OR COALESCE(
            stage_row."data"->'exclude_from_metrics',
            'false'::jsonb
          ) = 'true'::jsonb
        ) AS excluded,
        CASE
          WHEN jsonb_typeof(stage_row."data"->'is_small_business') = 'boolean'
            THEN (stage_row."data"->>'is_small_business')::boolean
          ELSE NULL
        END AS before_value,
        stage_row."data"->'is_small_business' AS current_value,
        stage_row."data"->>'small_business_evidence_id' AS before_evidence,
        stage_row."data"->>'small_business_source' AS current_source,
        stage_row."data"->>'small_business_outcome' AS current_outcome
      FROM "tbl_ptrs_stage_row" stage_row
      WHERE stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
    ),
    eligible AS MATERIALIZED (
      SELECT
        stage_source."id",
        stage_source."rowNo",
        stage_source.abn,
        CASE
          WHEN result."outcome" = :smallOutcome THEN true
          WHEN result."outcome" = :notSmallOutcome THEN false
          ELSE NULL
        END AS expected,
        result."outcome" AS outcome,
        stage_source.before_value,
        stage_source.before_evidence,
        'sbi:' || :uploadId || ':' || CASE
          WHEN result."outcome" = :smallOutcome THEN 'true'
          ELSE 'false'
        END AS history_key,
        (
          stage_source.current_value
            IS DISTINCT FROM to_jsonb(CASE
              WHEN result."outcome" = :smallOutcome THEN true
              ELSE false
            END)
          OR stage_source.before_evidence
            IS DISTINCT FROM :uploadId
          OR stage_source.current_source
            IS DISTINCT FROM 'SBI_UPLOAD'
          OR stage_source.current_outcome
            IS DISTINCT FROM result."outcome"
        ) AS data_changed,
        existing_changes."paymentRowId" IS NOT NULL
          AS change_record_exists
      FROM stage_source
      JOIN "tbl_ptrs_sbi_result" result
        ON result."customerId" = :customerId
       AND result."ptrsId" = :ptrsId
       AND result."sbiUploadId" = :uploadId
       AND result."deletedAt" IS NULL
       AND result."abn" = stage_source.abn
      LEFT JOIN existing_changes
        ON existing_changes."paymentRowId" = stage_source."id"
      WHERE NOT stage_source.excluded
        AND stage_source.abn ~ '^\\d{11}$'
        AND result."isValidAbn" IS TRUE
        AND result."outcome" IN (:smallOutcome, :notSmallOutcome)
    ),
    candidate_ids AS MATERIALIZED (
      SELECT
        eligible.*
      FROM eligible
      WHERE eligible.data_changed OR NOT eligible.change_record_exists
    ),
    candidate AS MATERIALIZED (
      SELECT
        candidate_ids.*,
        stage_row."data" AS current_data,
        stage_row."meta" AS current_meta,
        CASE
          WHEN candidate_ids.change_record_exists THEN false
          ELSE NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(
                COALESCE(stage_row."meta", '{}'::jsonb)
                  ->'transformationHistory'
              ) = 'array'
                THEN COALESCE(stage_row."meta", '{}'::jsonb)
                  ->'transformationHistory'
              ELSE '[]'::jsonb
            END
          ) history_item
          WHERE history_item->>'key' = candidate_ids.history_key
        ) END AS history_changed
      FROM candidate_ids
      JOIN "tbl_ptrs_stage_row" stage_row
        ON stage_row."id" = candidate_ids."id"
       AND stage_row."customerId" = :customerId
       AND stage_row."ptrsId" = :ptrsId
       AND stage_row."deletedAt" IS NULL
    ),
    changes_inserted AS (
      INSERT INTO "tbl_ptrs_sbi_row_change" (
        "id", "customerId", "ptrsId", "sbiUploadId", "paymentRowId",
        "supplierAbn", "beforeIsSmallBusiness", "afterIsSmallBusiness",
        "outcome", "changedBy", "changedAt", "createdAt", "updatedAt",
        "deletedAt"
      )
      SELECT
        translate(
          substr(
            encode(
              decode(
                md5(
                  candidate."id" || clock_timestamp()::text || random()::text
                ),
                'hex'
              ),
              'base64'
            ),
            1,
            10
          ),
          '/+',
          '_-'
        ),
        :customerId,
        :ptrsId,
        :uploadId,
        candidate."id",
        candidate.abn,
        candidate.before_value,
        candidate.expected,
        candidate.outcome,
        :userId,
        :checkedAt::timestamptz,
        now(),
        now(),
        NULL
      FROM candidate
      WHERE candidate.data_changed
        AND NOT candidate.change_record_exists
      RETURNING 1
    ),
    updated AS (
      UPDATE "tbl_ptrs_stage_row" stage_row
      SET
        "data" = CASE
          WHEN candidate.data_changed THEN
            COALESCE(stage_row."data", '{}'::jsonb) || jsonb_build_object(
              'is_small_business', candidate.expected,
              'small_business_outcome', candidate.outcome,
              'small_business_source', 'SBI_UPLOAD',
              'small_business_evidence_id', :uploadId,
              'small_business_checked_at', CASE
                WHEN candidate.before_evidence = :uploadId
                  THEN COALESCE(
                    stage_row."data"->>'small_business_checked_at',
                    :checkedAt
                  )
                ELSE :checkedAt
              END
            )
          ELSE stage_row."data"
        END,
        "meta" = CASE
          WHEN candidate.history_changed THEN ${nextMetaSql}
          ELSE stage_row."meta"
        END,
        "updatedAt" = now()
      FROM candidate
      WHERE stage_row."id" = candidate."id"
        AND stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
      RETURNING
        candidate."id",
        candidate."rowNo",
        candidate.abn,
        candidate.before_value AS "beforeIsSmallBusiness",
        candidate.expected AS "afterIsSmallBusiness",
        candidate.outcome,
        candidate.data_changed AS "dataChanged",
        candidate.history_changed AS "historyChanged"
    )
    SELECT
      (SELECT COUNT(*)::int FROM changes_inserted) AS "affectedRows",
      COUNT(*) FILTER (WHERE updated."historyChanged")::int AS "historyRows"
    FROM updated
  `;
}

async function applySbiResultsToStageRowsSql({
  customerId,
  ptrsId,
  userId,
  uploadId,
  transaction,
}) {
  const replacements = {
    customerId,
    ptrsId,
    uploadId,
    smallOutcome: OUTCOME_SMALL,
    notSmallOutcome: OUTCOME_NOT_SMALL,
  };
  const statsRows = await db.sequelize.query(buildSbiReapplyStatsSql(), {
    replacements,
    type: db.sequelize.QueryTypes.SELECT,
    transaction,
  });
  const sourceStats = statsRows?.[0] || {};
  const stats = {
    totalRows: Number(sourceStats.totalRows) || 0,
    excludedRows: Number(sourceStats.excludedRows) || 0,
    rowsWithPayeeAbn: Number(sourceStats.rowsWithPayeeAbn) || 0,
    matchedAbns: Number(sourceStats.matchedAbns) || 0,
    affectedRows: 0,
    historyRows: 0,
    missingAbnRows: Number(sourceStats.missingAbnRows) || 0,
    invalidMatchRows: Number(sourceStats.invalidMatchRows) || 0,
    unknownOutcomeRows: Number(sourceStats.unknownOutcomeRows) || 0,
  };
  const dataChangeRows = Number(sourceStats.dataChangeRows) || 0;
  const historyCheckRows = Number(sourceStats.historyCheckRows) || 0;
  if (dataChangeRows === 0 && historyCheckRows === 0) {
    return stats;
  }
  const checkedAt = new Date().toISOString();
  const updateRows = await db.sequelize.query(buildSbiReapplySql(), {
    replacements: {
      ...replacements,
      userId: userId || null,
      checkedAt,
    },
    type: db.sequelize.QueryTypes.SELECT,
    transaction,
  });
  const updateStats = updateRows?.[0] || {};
  stats.affectedRows = Number(updateStats.affectedRows) || 0;
  stats.historyRows = Number(updateStats.historyRows) || 0;

  return stats;
}

async function reapplyLatestResults({ customerId, ptrsId, userId = null }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const latestUpload = await getLatestAppliedUpload({
      customerId,
      ptrsId,
      transaction: t,
    });
    if (!latestUpload) {
      await t.commit();
      return {
        status: "MISSING",
        ptrsId,
        sbiUploadId: null,
        counts: { affectedRows: 0, historyRows: 0 },
      };
    }

    const counts = await applySbiResultsToStageRowsSql({
      customerId,
      ptrsId,
      userId,
      uploadId: latestUpload.id,
      transaction: t,
    });
    await t.commit();
    return {
      status: latestUpload.status,
      ptrsId,
      sbiUploadId: latestUpload.id,
      counts,
    };
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

function sha256(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

/**
 * Minimal CSV parser that supports commas inside quoted values.
 * This is intentionally small and purpose-built for the SBI tool outputs.
 */
function parseCsv(text) {
  const rows = [];
  const lines = String(text)
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((l) => l.trim() !== "");

  for (const line of lines) {
    const out = [];
    let cur = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i += 1) {
      const ch = line[i];

      if (ch === '"') {
        // Handle escaped quotes
        if (inQuotes && line[i + 1] === '"') {
          cur += '"';
          i += 1;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }

      if (ch === "," && !inQuotes) {
        out.push(cur);
        cur = "";
        continue;
      }

      cur += ch;
    }

    out.push(cur);
    rows.push(out.map((v) => String(v).trim()));
  }

  return rows;
}

function headerIndex(headers, candidates) {
  const lowered = headers.map((h) => String(h).trim().toLowerCase());
  for (const c of candidates) {
    const idx = lowered.indexOf(c.toLowerCase());
    if (idx >= 0) return idx;
  }
  return -1;
}

function classifyOutcome(outcomeRaw) {
  const outcome = String(outcomeRaw || "").trim();

  if (outcome === OUTCOME_SMALL) {
    return { isSmallBusiness: true, isValidAbn: true, outcome };
  }

  if (outcome === OUTCOME_NOT_SMALL) {
    return { isSmallBusiness: false, isValidAbn: true, outcome };
  }

  if (/not recognised as a valid abn/i.test(outcome)) {
    return { isSmallBusiness: null, isValidAbn: false, outcome };
  }

  return { isSmallBusiness: null, isValidAbn: true, outcome };
}

async function getLatestUpload({ customerId, ptrsId, transaction }) {
  return db.PtrsSbiUpload.findOne({
    where: { customerId, ptrsId },
    order: [["createdAt", "DESC"]],
    attributes: [
      "id",
      "status",
      "fileName",
      "fileHash",
      "rawRowCount",
      "parsedAbnCount",
      "summary",
      "createdAt",
    ],
    raw: true,
    transaction,
  });
}

async function getStatus({ customerId, ptrsId }) {
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const latestUpload = await getLatestUpload({
      customerId,
      ptrsId,
      transaction: t,
    });

    await t.commit();

    return {
      ptrsId,
      latestUpload: latestUpload
        ? {
            id: latestUpload.id,
            status: latestUpload.status,
            fileName: latestUpload.fileName,
            fileHash: latestUpload.fileHash,
            rawRowCount: latestUpload.rawRowCount,
            parsedAbnCount: latestUpload.parsedAbnCount,
            summary: latestUpload.summary || null,
            createdAt: latestUpload.createdAt,
          }
        : null,
    };
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}

function buildSbiExportSql() {
  return `
    WITH abns AS (
      SELECT DISTINCT
        NULLIF(
          regexp_replace(
            COALESCE(stage_row."data"->>'payee_entity_abn', ''),
            '\\D',
            '',
            'g'
          ),
          ''
        ) AS abn
      FROM "tbl_ptrs_stage_row" stage_row
      WHERE stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
        AND COALESCE(
          stage_row."meta"->'rules'->'exclude',
          'false'::jsonb
        ) <> 'true'::jsonb
        AND COALESCE(
          stage_row."data"->'exclude',
          'false'::jsonb
        ) <> 'true'::jsonb
        AND COALESCE(
          stage_row."data"->'exclude_from_metrics',
          'false'::jsonb
        ) <> 'true'::jsonb
    )
    SELECT
      'ABN'
      || COALESCE(
        E'\\n' || string_agg(abns.abn, E'\\n' ORDER BY abns.abn),
        ''
      )
      || E'\\n' AS "csvText"
    FROM abns
    WHERE abns.abn ~ '^\\d{11}$'
  `;
}

async function exportAbnCsv({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Ensure ptrs exists for tenant
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      attributes: ["id"],
      raw: true,
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    const exportRows = await db.sequelize.query(buildSbiExportSql(), {
      replacements: { customerId, ptrsId },
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    const csvText = exportRows?.[0]?.csvText || "ABN\n";

    await t.commit();

    return csvText;
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}

async function importResults({ customerId, ptrsId, userId, file }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!file?.buffer) throw new Error("file buffer is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Ensure ptrs exists for tenant
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      attributes: ["id"],
      raw: true,
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    const fileHash = sha256(file.buffer);
    const fileName = file.originalname || null;

    // Parse
    const text = file.buffer.toString("utf8");
    const parsed = parseCsv(text);

    if (!parsed.length) {
      const e = new Error("SBI results file is empty");
      e.statusCode = 400;
      throw e;
    }

    const headers = parsed[0];

    const abnIdx = headerIndex(headers, [
      "abn",
      "supplier abn",
      "entity abn",
      "payee_entity_abn",
    ]);
    const outcomeIdx = headerIndex(headers, ["outcome", "result", "status"]);
    const yearIdx = headerIndex(headers, ["year", "reporting year"]);

    if (abnIdx < 0 || outcomeIdx < 0) {
      const e = new Error(
        "SBI results CSV must include columns for ABN and Outcome (e.g. headers: Year, ABN, Outcome)",
      );
      e.statusCode = 400;
      throw e;
    }

    const rows = parsed.slice(1);

    const byAbn = new Map();

    let invalidAbns = 0;
    let unknownOutcomes = 0;

    for (const r of rows) {
      const abn = normalizeAbn(r[abnIdx] || "");
      if (!abn) continue;

      const outcomeRaw = r[outcomeIdx] || "";
      const yearRaw = yearIdx >= 0 ? r[yearIdx] : null;

      const { isSmallBusiness, isValidAbn, outcome } =
        classifyOutcome(outcomeRaw);
      if (!isValidAbn) invalidAbns += 1;
      if (
        isSmallBusiness == null &&
        isValidAbn &&
        outcome &&
        outcome !== OUTCOME_SMALL &&
        outcome !== OUTCOME_NOT_SMALL
      ) {
        unknownOutcomes += 1;
      }

      const year =
        yearRaw != null && String(yearRaw).trim() !== ""
          ? Number(String(yearRaw).trim())
          : null;

      // Deduplicate within upload (last row wins)
      byAbn.set(abn, {
        abn,
        outcome,
        year: Number.isFinite(year) ? year : null,
        isValidAbn,
      });
    }

    const parsedAbns = byAbn.size;

    if (parsedAbns === 0) {
      const e = new Error("No ABNs could be parsed from the SBI results file");
      e.statusCode = 400;
      throw e;
    }

    // Create upload anchor
    const uploadRow = await db.PtrsSbiUpload.create(
      {
        customerId,
        ptrsId,
        fileName,
        fileHash,
        rawRowCount: rows.length,
        parsedAbnCount: parsedAbns,
        status: "APPLIED",
        summary: null,
        uploadedBy: userId || null,
        appliedBy: userId || null,
      },
      { transaction: t },
    );

    // Insert results
    const resultRows = Array.from(byAbn.values()).map((r) => ({
      customerId,
      ptrsId,
      sbiUploadId: uploadRow.id,
      abn: r.abn,
      outcome: r.outcome,
      year: r.year,
      isValidAbn: r.isValidAbn,
    }));

    await db.PtrsSbiResult.bulkCreate(resultRows, {
      transaction: t,
      validate: false,
    });

    const stageCounts = await applySbiResultsToStageRowsSql({
      customerId,
      ptrsId,
      userId,
      uploadId: uploadRow.id,
      transaction: t,
    });
    const {
      totalRows,
      excludedRows,
      rowsWithPayeeAbn,
      matchedAbns,
      affectedRows,
      historyRows,
      missingAbnRows,
      invalidMatchRows,
      unknownOutcomeRows,
    } = stageCounts;

    // Decide status
    // MVP rule: unknown outcomes are BLOCKED; invalid matches are WARNINGS unless they match stage rows (we count invalidMatchRows).
    let status = "APPLIED";
    const blockingReasons = [];

    if (unknownOutcomeRows > 0) {
      status = "BLOCKED";
      blockingReasons.push(
        "Unknown SBI outcome values were encountered for matched stage rows",
      );
    }

    if (missingAbnRows > 0) {
      // Don’t block (yet). Validate will block if you keep strict rules.
      // We still flag as warning.
      if (status !== "BLOCKED") status = "APPLIED_WITH_WARNINGS";
    }

    if (invalidMatchRows > 0) {
      if (status !== "BLOCKED") status = "APPLIED_WITH_WARNINGS";
    }

    const summary = {
      fileName,
      fileHash,
      rawRowCount: rows.length,
      parsedAbns,
      invalidAbns,
      unknownOutcomes,
      stage: {
        totalRows,
        excludedRows,
        rowsWithPayeeAbn,
        matchedAbns,
        affectedRows,
        historyRows,
        missingAbnRows,
        invalidMatchRows,
        unknownOutcomeRows,
      },
      blockingReasons,
    };

    await db.PtrsSbiUpload.update(
      { status, summary, parsedAbnCount: parsedAbns, rawRowCount: rows.length },
      { where: { id: uploadRow.id, customerId }, transaction: t },
    );

    await t.commit();

    return {
      status,
      ptrsId,
      sbiUploadId: uploadRow.id,
      counts: {
        rawRows: rows.length,
        parsedAbns,
        invalidAbns,
        unknownOutcomes,
        totalStageRows: totalRows,
        excludedRows,
        rowsWithPayeeAbn,
        matchedAbns,
        affectedRows,
        historyRows,
        missingAbnRows,
        invalidMatchRows,
        unknownOutcomeRows,
      },
      summary,
    };
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}
