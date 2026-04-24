const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog } = require("./ptrs.service");

// Helper functions
const { getExclusionsSummary } = require("./exclusions.summary");
const {
  listKeywordExclusions,
  createKeywordExclusion,
  updateKeywordExclusion,
  deleteKeywordExclusion,
} = require("./exclusions.keywords");

const {
  applyKeywordExclusion,
  previewKeywordExclusion,
} = require("./exclusions.keyword.engine");

const {
  applyDocTypeExclusion,
  previewDocTypeExclusion,
} = require("./exclusions.docType");

const {
  applyEmployeeExclusion,
  previewEmployeeExclusion,
} = require("./exclusions.employee");
const { applyGovExclusion, previewGovExclusion } = require("./exclusions.gov");
const {
  applyIntraCompanyExclusion,
  previewIntraCompanyExclusion,
} = require("./exclusions.intraCompany");

const {
  applyPrepaidExclusion,
  previewPrepaidExclusion,
} = require("./exclusions.prepaid");

const {
  applyPaymentTermsExclusion,
  previewPaymentTermsExclusion,
} = require("./exclusions.paymentTerms");

const {
  applyInternationalExclusion,
  previewInternationalExclusion,
} = require("./exclusions.international");

/**
 * Exclusions are eligibility decisions, not transformations.
 * Canonical pattern: SQL-first updates against first-class columns on
 * tbl_ptrs_stage_row, never destroy/rebuild stage rows for exclusions.
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

    if (category === "all" || category === "gov") {
      stats.checksRun += 1;
      const affected = await applyGovExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
      });
      stats.rowsExcluded += affected;
    }

    if (category === "all" || category === "intra_company") {
      stats.checksRun += 1;
      const affected = await applyIntraCompanyExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        profileId,
      });
      stats.rowsExcluded += affected;
    }

    // Employee & expense payments (profile-scoped ref list; keyword match)
    if (category === "all" || category === "employee") {
      stats.checksRun += 1;
      const affected = await applyEmployeeExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        profileId,
      });
      stats.rowsExcluded += affected;
    }

    // Document type exclusions
    if (category === "all" || category === "doc_type") {
      stats.checksRun += 1;
      const affected = await applyDocTypeExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
      });
      stats.rowsExcluded += affected;
    }

    // Keyword exclusions (profile-scoped keyword list)
    if (category === "all" || category === "keyword") {
      stats.checksRun += 1;
      const affected = await applyKeywordExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        profileId,
      });
      stats.rowsExcluded += affected;
    }

    // Pre-payments (heuristic match on promoted payment-term fields)
    if (category === "all" || category === "prepaid") {
      stats.checksRun += 1;
      const affected = await applyPrepaidExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
      });
      stats.rowsExcluded += affected;
    }

    if (category === "all" || category === "payment_terms") {
      stats.checksRun += 1;
      const affected = await applyPaymentTermsExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
      });
      stats.rowsExcluded += affected;
    }

    // International suppliers (non-AUD document currency)
    if (category === "all" || category === "international") {
      stats.checksRun += 1;
      const affected = await applyInternationalExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
      });
      stats.rowsExcluded += affected;
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
      const govPreview = await previewGovExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        effectiveLimit,
      });

      result.counts.gov = govPreview.matched;
      result.alreadyExcludedCounts.gov = govPreview.alreadyExcluded;
      result.samples.gov = govPreview.sampleRows;
    }

    if (category === "all" || category === "intra_company") {
      stats.checksRun += 1;
      const intraCompanyPreview = await previewIntraCompanyExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        profileId,
        effectiveLimit,
      });

      result.counts.intra_company = intraCompanyPreview.matched;
      result.alreadyExcludedCounts.intra_company =
        intraCompanyPreview.alreadyExcluded;
      result.samples.intra_company = intraCompanyPreview.sampleRows;
    }

    if (category === "all" || category === "employee") {
      stats.checksRun += 1;
      const employeePreview = await previewEmployeeExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        profileId,
        effectiveLimit,
      });

      result.counts.employee = employeePreview.matched;
      result.alreadyExcludedCounts.employee = employeePreview.alreadyExcluded;
      result.samples.employee = employeePreview.sampleRows;
    }

    if (category === "all" || category === "doc_type") {
      stats.checksRun += 1;
      const docTypePreview = await previewDocTypeExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        effectiveLimit,
      });

      result.counts.doc_type = docTypePreview.matched;
      result.alreadyExcludedCounts.doc_type = docTypePreview.alreadyExcluded;
      result.samples.doc_type = docTypePreview.sampleRows;
    }

    // Keyword exclusions preview (profile-scoped keyword list)
    if (category === "all" || category === "keyword") {
      stats.checksRun += 1;
      const keywordPreview = await previewKeywordExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        profileId,
        effectiveLimit,
      });

      result.counts.keyword = keywordPreview.matched;
      result.alreadyExcludedCounts.keyword = keywordPreview.alreadyExcluded;
      result.samples.keyword = keywordPreview.sampleRows;
    }

    if (category === "all" || category === "prepaid") {
      stats.checksRun += 1;
      const prepaidPreview = await previewPrepaidExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        effectiveLimit,
      });

      result.counts.prepaid = prepaidPreview.matched;
      result.alreadyExcludedCounts.prepaid = prepaidPreview.alreadyExcluded;
      result.samples.prepaid = prepaidPreview.sampleRows;
    }

    if (category === "all" || category === "payment_terms") {
      stats.checksRun += 1;
      const paymentTermsPreview = await previewPaymentTermsExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        effectiveLimit,
      });

      result.counts.payment_terms = paymentTermsPreview.matched;
      result.alreadyExcludedCounts.payment_terms =
        paymentTermsPreview.alreadyExcluded;
      result.samples.payment_terms = paymentTermsPreview.sampleRows;
    }

    if (category === "all" || category === "international") {
      stats.checksRun += 1;
      const internationalPreview = await previewInternationalExclusion({
        sequelize,
        transaction: t,
        customerId,
        ptrsId,
        effectiveLimit,
      });

      result.counts.international = internationalPreview.matched;
      result.alreadyExcludedCounts.international =
        internationalPreview.alreadyExcluded;
      result.samples.international = internationalPreview.sampleRows;
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
  getExclusionsSummary,
  listKeywordExclusions,
  createKeywordExclusion,
  updateKeywordExclusion,
  deleteKeywordExclusion,
};
