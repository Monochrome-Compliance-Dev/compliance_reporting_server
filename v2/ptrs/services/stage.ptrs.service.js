const db = require("@/db/database");

const {
  safeMeta,
  slog,
  createExecutionRun,
  updateExecutionRun,
  getLatestExecutionRun,
} = require("./ptrs.service");

const { applyRules } = require("./rules.ptrs.service");
const { loadMappedRowsForPtrs } = require("./maps.ptrs.service");
const { getColumnMap } = require("@/v2/ptrs/services/maps.config.ptrs.service");
const {
  PTRS_CANONICAL_CONTRACT,
} = require("@/v2/ptrs/contracts/ptrs.canonical.contract");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { createPtrsTrace, hrMsSince } = require("@/helpers/ptrsTrackerLog");

const {
  getStageStaleness: getStageStalenessImpl,
  getStageCompletionGate: getStageCompletionGateImpl,
} = require("@/v2/ptrs/services/stage.staleness.ptrs.service");

const {
  toSnakeCase,
  collectCanonicalContractFields,
  computePaymentTimeRegulator,
} = require("@/v2/ptrs/services/stage.payment-time.ptrs.service");

const {
  loadEffectiveTermChangesForRows,
  applyEffectiveTermChangesToRows,
  loadPaymentTermMap,
  applyPaymentTermDaysFromMap,
} = require("@/v2/ptrs/services/stage.payment-terms.ptrs.service");

const {
  stagePtrs: stagePtrsImpl,
} = require("@/v2/ptrs/services/stage.build.ptrs.service");

const {
  getStagePreview: getStagePreviewImpl,
} = require("@/v2/ptrs/services/stage.preview.ptrs.service");

module.exports = {
  stagePtrs,
  getStagePreview,
  getStageStaleness,
  getStageCompletionGate,
};

async function stagePtrs({
  customerId,
  ptrsId,
  steps = [],
  persist = false,
  limit = null,
  userId,
  profileId = null,
  force = false,
}) {
  return stagePtrsImpl({
    customerId,
    ptrsId,
    steps,
    persist,
    limit,
    userId,
    profileId,
    force,
    beginTransactionWithCustomerContext,
    createPtrsTrace,
    hrMsSince,
    safeMeta,
    slog,
    getStageStaleness: getStageStalenessImpl,
    getLatestExecutionRun,
    createExecutionRun,
    updateExecutionRun,
    loadMappedRowsForPtrs,
    getColumnMap,
    applyRules,
    loadEffectiveTermChangesForRows,
    applyEffectiveTermChangesToRows,
    loadPaymentTermMap,
    applyPaymentTermDaysFromMap,
    computePaymentTimeRegulator,
    collectCanonicalContractFields,
    PTRS_CANONICAL_CONTRACT,
    toSnakeCase,
    db,
  });
}

async function getStagePreview({
  customerId,
  ptrsId,
  limit = 50,
  profileId = null,
}) {
  return getStagePreviewImpl({
    customerId,
    ptrsId,
    limit,
    profileId,
    beginTransactionWithCustomerContext,
    createPtrsTrace,
    hrMsSince,
    safeMeta,
    slog,
    db,
  });
}

async function getStageStaleness({ customerId, ptrsId, profileId }) {
  return getStageStalenessImpl({
    customerId,
    ptrsId,
    profileId,
  });
}

async function getStageCompletionGate({
  customerId,
  ptrsId,
  profileId = null,
}) {
  return getStageCompletionGateImpl({
    customerId,
    ptrsId,
    profileId,
  });
}
