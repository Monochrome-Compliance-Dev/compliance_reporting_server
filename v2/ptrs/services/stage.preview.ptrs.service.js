const {
  PTRS_CANONICAL_CONTRACT,
} = require("@/v2/ptrs/contracts/ptrs.canonical.contract");

const STAGE_PREVIEW_DERIVED_FIELDS = [
  "payment_term_days",
  "payment_time_days",
  "payment_time_reference_date",
  "payment_time_reference_kind",
  "contract_po_payment_terms_effective",
  "contract_po_payment_terms_effective_source",
  "contract_po_payment_terms_effective_changed_at",
];

function dedupeFields(fields) {
  const out = [];
  const seen = new Set();

  for (const field of Array.isArray(fields) ? fields : []) {
    if (!field || seen.has(field)) continue;
    seen.add(field);
    out.push(field);
  }

  return out;
}

function toSnakeCase(value) {
  if (!value) return "";
  return String(value)
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function toCamelCase(value) {
  const snake = toSnakeCase(value);
  if (!snake) return "";
  return snake.replace(/_([a-z0-9])/g, (_, ch) => ch.toUpperCase());
}

function toTitleWords(value) {
  const snake = toSnakeCase(value);
  if (!snake) return "";
  return snake
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function normaliseComparableKey(value) {
  if (!value) return "";
  return String(value)
    .replace(/[^a-zA-Z0-9]+/g, "")
    .toLowerCase();
}

function buildComparableIndex(source) {
  const index = new Map();
  const row = source && typeof source === "object" ? source : {};

  for (const key of Object.keys(row)) {
    const comparable = normaliseComparableKey(key);
    if (!comparable || index.has(comparable)) continue;
    index.set(comparable, key);
  }

  return index;
}

function buildFieldVariants(field) {
  const original = String(field || "").trim();
  const snake = toSnakeCase(original);
  const camel = toCamelCase(original);
  const title = toTitleWords(original);

  return dedupeFields([
    original,
    snake,
    camel,
    title,
    title.replace(/ /g, "_"),
    title.replace(/ /g, ""),
  ]);
}

function getValueByVariants(source, field) {
  const row = source && typeof source === "object" ? source : {};
  const variants = buildFieldVariants(field);

  for (const variant of variants) {
    if (Object.prototype.hasOwnProperty.call(row, variant)) {
      return row[variant];
    }
  }

  const comparableIndex = buildComparableIndex(row);
  for (const variant of variants) {
    const comparable = normaliseComparableKey(variant);
    const actualKey = comparableIndex.get(comparable);
    if (actualKey && Object.prototype.hasOwnProperty.call(row, actualKey)) {
      return row[actualKey];
    }
  }

  return null;
}

function collectContractPreviewFields(contract) {
  if (!contract || typeof contract !== "object") return [];

  const sections = [
    contract.identity,
    contract.transaction,
    contract.dates,
    contract.terms,
    contract.regulator_flags,
  ];

  const fields = [];
  const seen = new Set();

  for (const section of sections) {
    if (!section || typeof section !== "object") continue;
    for (const key of Object.keys(section)) {
      if (!key || seen.has(key)) continue;
      seen.add(key);
      fields.push(key);
    }
  }

  return fields;
}

function projectStagePreviewRow(row, allowedFields) {
  const source = row && typeof row === "object" ? row : {};
  const out = {};

  for (const field of allowedFields) {
    if (!field) continue;
    out[field] = getValueByVariants(source, field);
  }

  return out;
}

async function getStagePreview({
  customerId,
  ptrsId,
  limit = 50,
  profileId = null,
  beginTransactionWithCustomerContext,
  createPtrsTrace,
  hrMsSince,
  safeMeta,
  slog,
  db,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  const trace = createPtrsTrace({
    customerId,
    ptrsId,
    actorId: null,
    logInfo: (msg, meta) => slog.info(msg, meta),
    meta: safeMeta,
  });
  const startNs = process.hrtime.bigint();
  trace?.write("stage_preview_begin", { limit });

  let canon = [];
  try {
    const where = { customerId, ptrsId, deletedAt: null };

    const [rowsRaw, totalRows] = await Promise.all([
      db.PtrsStageRow.findAll({
        where,
        attributes: ["rowNo", "data"],
        order: [["rowNo", "ASC"]],
        limit,
        raw: true,
        transaction: t,
      }),
      db.PtrsStageRow.count({ where, transaction: t }),
    ]);

    const dbRows = rowsRaw;

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

    let mappedPreviewFields = [];

    if (profileId && db.PtrsFieldMap) {
      try {
        const fm = await db.PtrsFieldMap.findAll({
          where: { customerId, ptrsId, profileId },
          attributes: ["canonicalField"],
          order: [["createdAt", "ASC"]],
          transaction: t,
          raw: true,
        });

        mappedPreviewFields = Array.isArray(fm)
          ? fm.map((r) => r.canonicalField).filter(Boolean)
          : [];
      } catch (_) {
        // ignore
      }
    }

    const previewFields = dedupeFields(
      mappedPreviewFields.length
        ? [...mappedPreviewFields, ...STAGE_PREVIEW_DERIVED_FIELDS]
        : [
            ...collectContractPreviewFields(PTRS_CANONICAL_CONTRACT),
            ...STAGE_PREVIEW_DERIVED_FIELDS,
          ],
    );

    const rows = dbRows.map((row) =>
      projectStagePreviewRow(materialiseObj(row?.data) || {}, previewFields),
    );

    canon = mappedPreviewFields.slice();

    if (canon.length) {
      canon.forEach((k) => headerSet.add(k));
    }

    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      Object.keys(row).forEach((k) => headerSet.add(k));
    }

    const allowedHeaderSet = new Set(previewFields);

    let headers;
    if (canon.length) {
      const canonSet = new Set(canon.filter((h) => allowedHeaderSet.has(h)));
      headers = [
        ...canon.filter((h) => headerSet.has(h) && allowedHeaderSet.has(h)),
        ...previewFields.filter((h) => headerSet.has(h) && !canonSet.has(h)),
      ];
    } else {
      headers = previewFields.filter((h) => headerSet.has(h));
    }

    trace?.write("stage_preview_before_commit", {
      rows: Array.isArray(rowsRaw) ? rowsRaw.length : 0,
      totalRows,
      headersCount: Array.isArray(headers) ? headers.length : 0,
      totalMs: hrMsSince(startNs),
    });
    await t.commit();
    trace?.write("stage_preview_committed", { totalMs: hrMsSince(startNs) });
    if (trace) await trace.close();

    return {
      headers,
      rows,
      totalRows,
      stats: null,
    };
  } catch (err) {
    trace?.write("stage_preview_error", {
      message: err?.message || null,
      statusCode: err?.statusCode || null,
      totalMs: hrMsSince(startNs),
    });
    if (!t.finished) {
      try {
        await t.rollback();
      } catch {
        // ignore rollback errors
      }
    }
    if (trace) await trace.close();
    throw err;
  }
}

module.exports = {
  getStagePreview,
};
