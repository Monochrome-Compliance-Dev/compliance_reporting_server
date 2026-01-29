const crypto = require("crypto");

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  importResults,
  getStatus,
  exportAbnCsv,
  validateAppliedSbi,
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
    raw: true,
    transaction,
  });
}

async function loadSbiMap({ customerId, ptrsId, sbiUploadId, transaction }) {
  const rows = await db.PtrsSbiResult.findAll({
    where: { customerId, ptrsId, sbiUploadId },
    raw: true,
    transaction,
  });

  const map = new Map();
  const invalid = new Set();

  for (const r of rows) {
    const abn = normalizeAbn(r.abn);
    if (!abn) continue;

    const outcome = String(r.outcome || "").trim();
    const isValidAbn = r.isValidAbn !== false;

    map.set(abn, { outcome, isValidAbn });

    if (!isValidAbn || /not recognised as a valid abn/i.test(outcome)) {
      invalid.add(abn);
    }
  }

  return { map, invalid, totalResults: rows.length };
}

function expectedSmallBusinessFromOutcome(outcome) {
  if (outcome === OUTCOME_SMALL) return true;
  if (outcome === OUTCOME_NOT_SMALL) return false;
  return null;
}

function toIssue(stageRow, code, message, extra = {}) {
  return {
    stageRowId: stageRow.id,
    rowNo: stageRow.rowNo,
    code,
    message,
    payeeAbn: normalizeAbn(stageRow?.data?.payee_entity_abn || ""),
    ...extra,
  };
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

    const blockers = [];
    const warnings = [];

    if (!latestSbi) {
      blockers.push({
        code: "SBI_MISSING",
        message:
          "SBI Check has not been applied for this PTRS run. Upload SBI results before validating.",
      });

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
          warnings: warnings.length,
        },
        blockers,
        warnings,
      };
    }

    const {
      map: sbiMap,
      invalid: invalidAbns,
      totalResults,
    } = await loadSbiMap({
      customerId,
      ptrsId,
      sbiUploadId: latestSbi.id,
      transaction: t,
    });

    const stageRows = await db.PtrsStageRow.findAll({
      where: { customerId, ptrsId, deletedAt: null },
      order: [["rowNo", "ASC"]],
      raw: false,
      transaction: t,
    });

    const LIMIT = 200;

    let excludedRows = 0;
    let missingPayeeAbnCount = 0;
    let invalidPayeeAbnCount = 0;
    let abnMissingFromSbiResultsCount = 0;
    let sbiOutcomeMismatchCount = 0;
    let sbiEvidenceMismatchCount = 0;

    for (const r of stageRows) {
      if (isExcludedRow(r)) {
        excludedRows += 1;
        continue;
      }

      const payeeAbn = normalizeAbn(r?.data?.payee_entity_abn);

      if (!payeeAbn) {
        missingPayeeAbnCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "PAYEE_ABN_MISSING", "Missing payee_entity_abn"),
          );
        }
        continue;
      }

      if (!isProbablyAbn(payeeAbn)) {
        invalidPayeeAbnCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "PAYEE_ABN_INVALID",
              "payee_entity_abn is not a valid 11-digit ABN",
            ),
          );
        }
        continue;
      }

      if (invalidAbns.has(payeeAbn)) {
        if (blockers.length < LIMIT) {
          const outcome = sbiMap.get(payeeAbn)?.outcome || null;
          blockers.push(
            toIssue(
              r,
              "SBI_INVALID_ABN",
              "SBI results indicate this ABN is invalid/unrecognised",
              { outcome },
            ),
          );
        }
        continue;
      }

      const sbi = sbiMap.get(payeeAbn);
      if (!sbi) {
        abnMissingFromSbiResultsCount += 1;
        if (warnings.length < LIMIT) {
          warnings.push(
            toIssue(
              r,
              "SBI_NO_MATCH",
              "No SBI outcome found for this payee ABN (possible mismatched SBI file)",
            ),
          );
        }
        continue;
      }

      const expected = expectedSmallBusinessFromOutcome(sbi.outcome);
      if (expected == null) {
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "SBI_UNKNOWN_OUTCOME", "SBI outcome is not recognised", {
              outcome: sbi.outcome,
            }),
          );
        }
        continue;
      }

      const actual = r?.data?.is_small_business;
      const evidenceId = r?.data?.small_business_evidence_id;

      if (evidenceId !== latestSbi.id) {
        sbiEvidenceMismatchCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "SBI_EVIDENCE_MISSING",
              "Row is missing the expected small business evidence id for the latest SBI upload",
              {
                expectedEvidenceId: latestSbi.id,
                actualEvidenceId: evidenceId || null,
              },
            ),
          );
        }
        continue;
      }

      if (actual !== expected) {
        sbiOutcomeMismatchCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "SBI_FLAG_MISMATCH",
              "Row small business flag does not match the SBI outcome",
              {
                outcome: sbi.outcome,
                expected,
                actual: actual == null ? null : !!actual,
              },
            ),
          );
        }
        continue;
      }
    }

    const status =
      blockers.length > 0
        ? "BLOCKED"
        : warnings.length > 0
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
        totalResults,
      },
      counts: {
        totalRows: stageRows.length,
        excludedRows,
        blockers: blockers.length,
        warnings: warnings.length,
        missingPayeeAbnCount,
        invalidPayeeAbnCount,
        abnMissingFromSbiResultsCount,
        sbiEvidenceMismatchCount,
        sbiOutcomeMismatchCount,
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

function isProbablyAbn(abn) {
  return typeof abn === "string" && /^\d{11}$/.test(abn);
}

function isExcludedRow(stageRow) {
  const meta = stageRow?.meta || {};
  return meta?.rules?.exclude === true;
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

async function exportAbnCsv({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Ensure ptrs exists for tenant
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    const stageRows = await db.PtrsStageRow.findAll({
      where: { customerId, ptrsId, deletedAt: null },
      raw: false,
      transaction: t,
    });

    const abns = new Set();

    for (const r of stageRows) {
      if (isExcludedRow(r)) continue;

      const payeeAbn = normalizeAbn(r?.data?.payee_entity_abn);
      if (!payeeAbn) continue;
      if (!isProbablyAbn(payeeAbn)) continue;

      abns.add(payeeAbn);
    }

    // Deterministic ordering
    const ordered = Array.from(abns).sort();

    // SBI tool typically expects headers Year, ABN, Outcome in the *response* file.
    // For the *export* file, MVP is a single ABN column.
    const lines = ["ABN", ...ordered];
    const csvText = `${lines.join("\n")}\n`;

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

    // Apply to stage rows
    const stageRows = await db.PtrsStageRow.findAll({
      where: { customerId, ptrsId, deletedAt: null },
      order: [["rowNo", "ASC"]],
      raw: false,
      transaction: t,
    });

    let excludedRows = 0;
    let rowsWithPayeeAbn = 0;
    let matchedAbns = 0;
    let affectedRows = 0;
    let missingAbnRows = 0;
    let invalidMatchRows = 0;
    let unknownOutcomeRows = 0;

    const nowIso = new Date().toISOString();
    const rowChanges = [];

    // Update sequentially for determinism; can be optimised later
    for (const stageRow of stageRows) {
      if (isExcludedRow(stageRow)) {
        excludedRows += 1;
        continue;
      }

      const payeeAbn = normalizeAbn(stageRow?.data?.payee_entity_abn);
      if (!payeeAbn) {
        missingAbnRows += 1;
        continue;
      }

      rowsWithPayeeAbn += 1;

      if (!isProbablyAbn(payeeAbn)) {
        // dataset issue; do not apply
        continue;
      }

      const sbi = byAbn.get(payeeAbn);
      if (!sbi) {
        continue;
      }

      matchedAbns += 1;

      if (!sbi.isValidAbn) {
        invalidMatchRows += 1;
        continue;
      }

      const expected = classifyOutcome(sbi.outcome).isSmallBusiness;
      if (expected == null) {
        unknownOutcomeRows += 1;
        continue;
      }

      const before = stageRow?.data?.is_small_business;
      const beforeEvidence = stageRow?.data?.small_business_evidence_id;

      const nextData = {
        ...(stageRow.data || {}),
        is_small_business: expected,
        small_business_outcome: sbi.outcome,
        small_business_source: "SBI_UPLOAD",
        small_business_evidence_id: uploadRow.id,
        small_business_checked_at: nowIso,
      };

      // Only write if something meaningfully changes (flag OR evidence)
      const shouldUpdate =
        before !== expected ||
        beforeEvidence !== uploadRow.id ||
        stageRow?.data?.small_business_source !== "SBI_UPLOAD";

      if (shouldUpdate) {
        affectedRows += 1;

        rowChanges.push({
          customerId,
          ptrsId,
          sbiUploadId: uploadRow.id,
          paymentRowId: stageRow.id,
          supplierAbn: payeeAbn,
          beforeIsSmallBusiness:
            before == null
              ? null
              : typeof before === "boolean"
                ? before
                : !!before,
          afterIsSmallBusiness: expected,
          outcome: sbi.outcome,
          changedBy: userId || null,
          changedAt: new Date(),
        });

        stageRow.data = nextData;
        await stageRow.save({ transaction: t });
      }
    }

    if (rowChanges.length) {
      await db.PtrsSbiRowChange.bulkCreate(rowChanges, {
        transaction: t,
        validate: false,
      });
    }

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
        totalRows: stageRows.length,
        excludedRows,
        rowsWithPayeeAbn,
        matchedAbns,
        affectedRows,
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
        totalStageRows: stageRows.length,
        excludedRows,
        rowsWithPayeeAbn,
        matchedAbns,
        affectedRows,
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
