function normaliseTransformationHistory(meta) {
  return Array.isArray(meta?.transformationHistory)
    ? meta.transformationHistory
    : [];
}

function appendTransformationHistory(meta, event) {
  const base = meta && typeof meta === "object" ? meta : {};
  const history = normaliseTransformationHistory(base);
  const key = String(event?.key || "").trim();

  if (!key || history.some((item) => item?.key === key)) return base;

  return {
    ...base,
    transformationHistory: [...history, event],
  };
}

function appendTransformationHistorySql(metaSql, eventSql) {
  const source = `COALESCE(${metaSql}, '{}'::jsonb)`;
  const event = `(${eventSql})`;

  return `
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(${source}->'transformationHistory') = 'array'
              THEN ${source}->'transformationHistory'
            ELSE '[]'::jsonb
          END
        ) history_item
        WHERE history_item->>'key' = ${event}->>'key'
      ) THEN ${source}
      ELSE jsonb_set(
        ${source},
        '{transformationHistory}',
        CASE
          WHEN jsonb_typeof(${source}->'transformationHistory') = 'array'
            THEN ${source}->'transformationHistory'
          ELSE '[]'::jsonb
        END || jsonb_build_array(${event}),
        true
      )
    END
  `;
}

function appendTransformationHistoryEventsSql(metaSql, eventsSql) {
  const source = `COALESCE(${metaSql}, '{}'::jsonb)`;
  const existing = `CASE
    WHEN jsonb_typeof(${source}->'transformationHistory') = 'array'
      THEN ${source}->'transformationHistory'
    ELSE '[]'::jsonb
  END`;
  const candidates = `COALESCE(${eventsSql}, '[]'::jsonb)`;

  return `
    jsonb_set(
      ${source},
      '{transformationHistory}',
      ${existing} || COALESCE((
        SELECT jsonb_agg(missing.event ORDER BY missing.ordinal)
        FROM (
          SELECT DISTINCT ON (candidate.event->>'key')
            candidate.event,
            candidate.ordinal
          FROM jsonb_array_elements(${candidates})
            WITH ORDINALITY candidate(event, ordinal)
          WHERE NULLIF(BTRIM(candidate.event->>'key'), '') IS NOT NULL
            AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(${existing}) history_item
              WHERE history_item->>'key' = candidate.event->>'key'
            )
          ORDER BY candidate.event->>'key', candidate.ordinal
        ) missing
      ), '[]'::jsonb),
      true
    )
  `;
}

module.exports = {
  appendTransformationHistory,
  appendTransformationHistoryEventsSql,
  appendTransformationHistorySql,
  normaliseTransformationHistory,
};
