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

module.exports = {
  appendTransformationHistory,
  appendTransformationHistorySql,
  normaliseTransformationHistory,
};
