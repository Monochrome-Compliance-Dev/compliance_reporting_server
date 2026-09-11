const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

const ABN_DIGITS_SQL = `NULLIF(regexp_replace(
  COALESCE(s."payeeEntityAbn", s."data"->>'payee_entity_abn', ''),
  '\\D', '', 'g'
), '')`;
const EXPLICIT_NO_ABN_MARKER_SQL = `UPPER(BTRIM(COALESCE(
  s."payeeEntityAbn", s."data"->>'payee_entity_abn', ''
))) = 'N/A'`;
const EXPLICIT_NO_ABN_SQL = `LOWER(BTRIM(COALESCE(
  s."data"->>'supplier_has_no_abn', ''
))) IN ('true', 'yes', '1') OR ${EXPLICIT_NO_ABN_MARKER_SQL}`;
const CHECKSUM_SQL = `CASE
  WHEN ${ABN_DIGITS_SQL} ~ '^\\d{11}$' THEN (
    (substring(${ABN_DIGITS_SQL}, 1, 1)::int - 1) * 10
    + substring(${ABN_DIGITS_SQL}, 2, 1)::int
    + substring(${ABN_DIGITS_SQL}, 3, 1)::int * 3
    + substring(${ABN_DIGITS_SQL}, 4, 1)::int * 5
    + substring(${ABN_DIGITS_SQL}, 5, 1)::int * 7
    + substring(${ABN_DIGITS_SQL}, 6, 1)::int * 9
    + substring(${ABN_DIGITS_SQL}, 7, 1)::int * 11
    + substring(${ABN_DIGITS_SQL}, 8, 1)::int * 13
    + substring(${ABN_DIGITS_SQL}, 9, 1)::int * 15
    + substring(${ABN_DIGITS_SQL}, 10, 1)::int * 17
    + substring(${ABN_DIGITS_SQL}, 11, 1)::int * 19
  ) % 89 = 0
  ELSE false
END`;

function buildAbnEligibilitySql() {
  const reasonSql = `CASE
    WHEN ${EXPLICIT_NO_ABN_SQL} THEN 'NO_ABN'
    WHEN ${ABN_DIGITS_SQL} IS NULL THEN 'MAPPING_EXCEPTION'
    WHEN ${ABN_DIGITS_SQL} !~ '^\\d{11}$' THEN 'INVALID_ABN'
    WHEN NOT ${CHECKSUM_SQL} THEN 'INVALID_ABN'
    ELSE 'ABN_NOT_CONFIRMED'
  END`;
  const commentSql = `CASE
    WHEN ${EXPLICIT_NO_ABN_SQL}
      THEN 'Supplier is explicitly identified as having no ABN'
    WHEN ${ABN_DIGITS_SQL} IS NULL
      THEN 'Supplier ABN is missing without an authoritative no-ABN indicator'
    WHEN ${ABN_DIGITS_SQL} !~ '^\\d{11}$'
      THEN 'Supplier ABN fails the Australian ABN structure/checksum'
    WHEN ${ABN_DIGITS_SQL} ~ '^\\d{11}$' AND NOT ${CHECKSUM_SQL}
      THEN 'Supplier ABN fails the Australian ABN structure/checksum'
    ELSE 'Checksum-valid supplier ABN could not be confirmed through the ABR process'
  END`;
  const dataSql = appendJsonbTextArray(
    "exclude_comment",
    commentSql,
    appendJsonbTextArray(
      "exclude_reasons",
      reasonSql,
      applyExcludeFlags('s."data"', reasonSql),
    ),
  );
  const metaSql = appendJsonbTextArrayAtPath(
    "exclusions,comments",
    commentSql,
    appendJsonbTextArrayAtPath(
      "exclusions,reasons",
      reasonSql,
      applyMetaBase('s."meta"'),
    ),
  );

  return `
    UPDATE "tbl_ptrs_stage_row" s
    SET "data" = ${dataSql}, "meta" = ${metaSql}, "updatedAt" = now()
    WHERE s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND (
        ${EXPLICIT_NO_ABN_SQL}
        OR ${ABN_DIGITS_SQL} IS NULL
        OR ${ABN_DIGITS_SQL} !~ '^\\d{11}$'
        OR (${ABN_DIGITS_SQL} ~ '^\\d{11}$' AND NOT ${CHECKSUM_SQL})
        OR EXISTS (
          SELECT 1 FROM "tbl_ptrs_abr_lookup_cache" cache
          WHERE cache."abn" = ${ABN_DIGITS_SQL}
            AND cache."classification" = 'ABN_NOT_CONFIRMED'
            AND cache."expiresAt" > now()
        )
      )
      AND NOT COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)
        @> jsonb_build_array((${reasonSql})::text)
  `;
}

async function applyAbnEligibilityExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const [, metadata] = await sequelize.query(buildAbnEligibilitySql(), {
    replacements: { customerId, ptrsId },
    transaction,
  });
  return Number(metadata?.rowCount || 0);
}

module.exports = {
  applyAbnEligibilityExclusion,
  buildAbnEligibilitySql,
};
