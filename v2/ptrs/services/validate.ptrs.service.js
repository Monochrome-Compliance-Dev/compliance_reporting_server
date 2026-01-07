const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  validate,
  getValidate,
};

const OUTCOME_SMALL = "Small business for payment times reporting";
const OUTCOME_NOT_SMALL = "Not a small business for payment times reporting";

function normalizeAbn(value) {
  if (value == null) return "";
  return String(value).replace(/\D+/g, "");
}

function isProbablyAbn(abn) {
  // MVP: SBI tool outcome already handles invalids; we only need a basic sanity check
  return typeof abn === "string" && /^\d{11}$/.test(abn);
}

function isExcludedRow(stageRow) {
  const meta = stageRow?.meta || {};
  return meta?.rules?.exclude === true;
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

async function getLatestSbiUpload({ customerId, ptrsId, transaction }) {
  // Prefer most recent upload with a usable status
  const row = await db.PtrsSbiUpload.findOne({
    where: {
      customerId,
      ptrsId,
      status: ["APPLIED", "APPLIED_WITH_WARNINGS"],
    },
    order: [["createdAt", "DESC"]],
    raw: true,
    transaction,
  });

  return row;
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

async function validate({ customerId, ptrsId, userId = null }) {
  // POST version: no persistence yet, but we keep it distinct for future.
  return computeValidate({ customerId, ptrsId, userId, mode: "run" });
}

async function getValidate({ customerId, ptrsId, userId = null }) {
  return computeValidate({ customerId, ptrsId, userId, mode: "read" });
}

async function computeValidate({ customerId, ptrsId, userId, mode }) {
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

    const latestSbi = await getLatestSbiUpload({
      customerId,
      ptrsId,
      transaction: t,
    });

    const blockers = [];
    const warnings = [];

    // Gate: SBI must have run successfully before Validate
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
      where: { customerId, ptrsId },
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
            toIssue(r, "PAYEE_ABN_MISSING", "Missing payee_entity_abn")
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
              "payee_entity_abn is not a valid 11-digit ABN"
            )
          );
        }
        continue;
      }

      // If SBI tool explicitly says this ABN is invalid and we have it in the dataset, block.
      if (invalidAbns.has(payeeAbn)) {
        if (blockers.length < LIMIT) {
          const outcome = sbiMap.get(payeeAbn)?.outcome || null;
          blockers.push(
            toIssue(
              r,
              "SBI_INVALID_ABN",
              "SBI results indicate this ABN is invalid/unrecognised",
              {
                outcome,
              }
            )
          );
        }
        continue;
      }

      const sbi = sbiMap.get(payeeAbn);
      if (!sbi) {
        // The SBI export should be generated from the dataset, so missing matches are suspicious.
        abnMissingFromSbiResultsCount += 1;
        if (warnings.length < LIMIT) {
          warnings.push(
            toIssue(
              r,
              "SBI_NO_MATCH",
              "No SBI outcome found for this payee ABN (possible mismatched SBI file)"
            )
          );
        }
        continue;
      }

      const expected = expectedSmallBusinessFromOutcome(sbi.outcome);
      if (expected == null) {
        // Unknown outcome text = treat as blocker (we can't interpret)
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "SBI_UNKNOWN_OUTCOME", "SBI outcome is not recognised", {
              outcome: sbi.outcome,
            })
          );
        }
        continue;
      }

      const actual = r?.data?.is_small_business;
      const evidenceId = r?.data?.small_business_evidence_id;

      // If the ABN was in SBI results, we expect the row to carry evidence id.
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
              }
            )
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
              }
            )
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
      // ignore rollback errors
    }
    throw err;
  }
}
