const PERSIST_BATCH_SIZE = 2000;

const {
  appendTransformationHistory,
} = require("./stage.transformation-history");

function parseRowRules(mapRow) {
  const raw = mapRow?.rowRules ?? mapRow?.extras?.rowRules ?? null;
  if (typeof raw !== "string") return Array.isArray(raw) ? raw : [];
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : [];
}

function collectRuleOutputFields(rowRules) {
  return Array.from(
    new Set(
      (rowRules || [])
        .flatMap((rule) => (Array.isArray(rule?.then) ? rule.then : []))
        .map((action) => action?.field)
        .filter(Boolean),
    ),
  );
}

function collectConfiguredCanonicalFields(mappings) {
  const entries = Array.isArray(mappings)
    ? mappings
    : mappings && typeof mappings === "object"
      ? Object.values(mappings)
      : [];

  return Array.from(
    new Set(
      entries
        .map((mapping) =>
          typeof mapping === "string"
            ? mapping
            : mapping?.canonicalField ||
              mapping?.canonical ||
              mapping?.field ||
              mapping?.to ||
              mapping?.target,
        )
        .filter(Boolean),
    ),
  );
}

function addStats(total, next) {
  if (!next) return total;
  if (!total) return { ...next };
  const out = { ...total };
  for (const [key, value] of Object.entries(next)) {
    if (key === "rulesTried" && typeof value === "number") {
      out[key] = Math.max(Number(out[key] || 0), value);
    } else if (typeof value === "number") {
      out[key] = Number(out[key] || 0) + value;
    } else if (out[key] == null) out[key] = value;
  }
  return out;
}

async function transformStageRows({
  rows,
  rowRules,
  customerId,
  profileId,
  mapRow,
  joinContext,
  transaction,
  applyRules,
  loadEffectiveTermChangesForRows,
  applyEffectiveTermChangesToRows,
  termMap,
  applyPaymentTermDaysFromMap,
  computePaymentTimeRegulator,
}) {
  let stagedRows = Array.isArray(rows) ? rows : [];
  const rulesResult = applyRules(stagedRows, rowRules);
  stagedRows = rulesResult.rows || stagedRows;

  let paymentTermChangeStats = null;
  let paymentTermStats = null;
  if (profileId) {
    const termsBefore = new Map(
      stagedRows.map((row) => [
        row,
        row?.contract_po_payment_terms_effective ??
          row?.invoice_payment_terms_effective ??
          null,
      ]),
    );
    const changeMap = await loadEffectiveTermChangesForRows({
      customerId,
      profileId,
      rows: stagedRows,
      mapRow,
      joinContext,
      transaction,
    });
    const changeResult = applyEffectiveTermChangesToRows(
      stagedRows,
      changeMap,
      mapRow,
      joinContext,
    );
    stagedRows = changeResult.rows || stagedRows;
    paymentTermChangeStats = changeResult.stats || null;

    for (const row of stagedRows) {
      if (row?.contract_po_payment_terms_effective_source !== "TERM_CHANGES") {
        continue;
      }
      const effectiveTerm = row.contract_po_payment_terms_effective;
      const changedAt = row.contract_po_payment_terms_effective_changed_at;
      const previousTerm = termsBefore.get(row) ?? null;
      row._transformationMeta = appendTransformationHistory(
        row._transformationMeta,
        {
          key: `payment-term-change:${changedAt || "unknown"}:${effectiveTerm}`,
          kind: "payment_term_override",
          comment: `Payment term overridden from ${previousTerm || "not supplied"} to ${effectiveTerm} by supplier term change effective on ${changedAt || "an unspecified date"}`,
          sourceStageRowIds: [],
          targetStageRowIds: [],
          details: {
            previousTerm,
            effectiveTerm,
            effectiveDate: changedAt || null,
            source: "TERM_CHANGES",
          },
        },
      );
    }

    const termResult = applyPaymentTermDaysFromMap(stagedRows, termMap);
    stagedRows = termResult.rows || stagedRows;
    paymentTermStats = termResult.stats || null;
  }

  let paymentTimeDerived = 0;
  let paymentTimeUnderived = 0;
  for (const row of stagedRows) {
    if (!row || typeof row !== "object") continue;
    const result = computePaymentTimeRegulator(row);
    if (result?.days == null) {
      if (row.payment_time_days == null) {
        if (!Array.isArray(row._stageErrors)) row._stageErrors = [];
        row._stageErrors.push({
          code: "PAYMENT_TIME_UNDERIVED",
          message:
            "Payment time could not be derived using regulator rules (missing required date fields)",
          field: "payment_time_days",
          value: null,
        });
      }
      paymentTimeUnderived += 1;
      continue;
    }
    row.payment_time_days = result.days;
    row.payment_time_reference_date = result.referenceDate || null;
    row.payment_time_reference_kind = result.referenceKind || null;
    row._transformationMeta = appendTransformationHistory(
      row._transformationMeta,
      {
        key: `payment-time:${result.referenceKind || "unknown"}:${result.referenceDate || "unknown"}:${result.days}`,
        kind: "payment_time",
        comment: `Payment-time reference chosen from ${String(result.referenceKind || "configured date").replaceAll("_", " ")} (${result.referenceDate || "date unavailable"}); derived payment time ${result.days} day(s)`,
        sourceStageRowIds: [],
        targetStageRowIds: [],
        details: {
          referenceKind: result.referenceKind || null,
          referenceDate: result.referenceDate || null,
          paymentTimeDays: result.days,
        },
      },
    );
    paymentTimeDerived += 1;
  }

  return {
    rows: stagedRows,
    stats: {
      rules: rulesResult.stats || null,
      paymentTerms: paymentTermStats,
      paymentTermChanges: paymentTermChangeStats,
      paymentTime: {
        derived: paymentTimeDerived,
        underived: paymentTimeUnderived,
      },
    },
  };
}

async function stagePtrs({
  customerId,
  ptrsId,
  steps = [],
  persist = false,
  limit = null,
  userId,
  profileId = null,
  force = false,
  beginTransactionWithCustomerContext,
  createPtrsTrace,
  hrMsSince,
  safeMeta,
  slog,
  getStageStaleness,
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
  buildPersistedStageRow,
  buildStageColumnProjection,
  db,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (persist && !profileId) {
    const error = new Error("profileId is required when persist=true");
    error.statusCode = 400;
    throw error;
  }

  const started = Date.now();
  const transaction = await beginTransactionWithCustomerContext(customerId);
  const jobStartNs = process.hrtime.bigint();
  const trace = createPtrsTrace({
    customerId,
    ptrsId,
    actorId: userId || null,
    logInfo: (message, meta) => slog.info(message, meta),
    meta: safeMeta,
  });
  let executionRun = null;
  let inputHash = null;
  const totalStats = {
    rules: null,
    paymentTerms: null,
    paymentTermChanges: null,
    paymentTime: null,
  };
  const accumulateStats = (stats) => {
    totalStats.rules = addStats(totalStats.rules, stats?.rules);
    totalStats.paymentTerms = addStats(
      totalStats.paymentTerms,
      stats?.paymentTerms,
    );
    totalStats.paymentTermChanges = addStats(
      totalStats.paymentTermChanges,
      stats?.paymentTermChanges,
    );
    totalStats.paymentTime = addStats(
      totalStats.paymentTime,
      stats?.paymentTime,
    );
  };

  trace?.write("stage_begin", {
    persist: !!persist,
    force: !!force,
    limit: limit == null ? null : Number(limit),
    stepsCount: Array.isArray(steps) ? steps.length : 0,
    hasProfileId: !!profileId,
  });

  try {
    if (persist) {
      const staleness = await getStageStaleness({
        customerId,
        ptrsId,
        profileId,
        transaction,
      });
      inputHash = staleness.inputHash;
      const previous = staleness.previousRunId
        ? {
            id: staleness.previousRunId,
            inputHash: staleness.previousHash,
            status: staleness.previousHash ? "success" : null,
          }
        : await getLatestExecutionRun({
            customerId,
            ptrsId,
            step: "stage",
            transaction,
          });
      if (
        !force &&
        previous?.status === "success" &&
        previous.inputHash === inputHash &&
        Number(staleness.existingStageCount) > 0
      ) {
        await transaction.commit();
        if (trace) await trace.close();
        return {
          skipped: true,
          reason: "INPUT_UNCHANGED",
          inputHash,
          previousRunId: previous.id || null,
          persistedCount: Number(staleness.existingStageCount),
          rowsIn: null,
          rowsOut: null,
          tookMs: Date.now() - started,
          sample: null,
          stats: null,
        };
      }
      executionRun = await createExecutionRun({
        customerId,
        ptrsId,
        profileId,
        step: "stage",
        inputHash,
        status: "running",
        startedAt: new Date(),
        createdBy: userId || null,
        transaction,
      });
    }

    const mapRow = await getColumnMap({ customerId, ptrsId, transaction });
    const rowRules = parseRowRules(mapRow);
    const termMap = profileId
      ? await loadPaymentTermMap({ customerId, profileId, transaction })
      : new Map();
    const [mappedFieldRows, joinDatasets] = await Promise.all([
      profileId && db.PtrsFieldMap
        ? db.PtrsFieldMap.findAll({
            where: { customerId, ptrsId, profileId },
            attributes: [
              "canonicalField",
              "sourceRole",
              "sourceColumn",
              "datasetId",
            ],
            raw: true,
            transaction,
          })
        : Promise.resolve([]),
      db.PtrsDataset.findAll({
        where: { customerId, ptrsId },
        attributes: ["id", "role"],
        raw: true,
        transaction,
      }),
    ]);
    const joinContext = {
      customerId,
      ptrsId,
      datasets: joinDatasets,
      fieldMapRows: mappedFieldRows,
    };
    const mappedFields = (mappedFieldRows || [])
      .map((row) => toSnakeCase(row?.canonicalField))
      .filter(Boolean);
    const persistedStageFields = Array.from(
      new Set([
        ...collectCanonicalContractFields(PTRS_CANONICAL_CONTRACT),
        ...mappedFields,
        ...collectConfiguredCanonicalFields(mapRow?.mappings).map(toSnakeCase),
        ...collectRuleOutputFields(rowRules).map(toSnakeCase),
        "payment_term_days",
        "payment_time_days",
        "payment_time_reference_date",
        "payment_time_reference_kind",
        "contract_po_payment_terms_effective",
        "contract_po_payment_terms_effective_source",
        "contract_po_payment_terms_effective_changed_at",
        "exclude_reason",
      ]),
    );

    const transform = async (rows) => {
      const result = await transformStageRows({
        rows,
        rowRules,
        customerId,
        profileId,
        mapRow,
        joinContext,
        transaction,
        applyRules,
        loadEffectiveTermChangesForRows,
        applyEffectiveTermChangesToRows,
        termMap,
        applyPaymentTermDaysFromMap,
        computePaymentTimeRegulator,
      });
      accumulateStats(result.stats);
      return result.rows;
    };

    let stagedRows = [];
    let rowsIn = 0;
    let rowsOut = 0;
    let persistedCount = null;
    if (!persist) {
      const loaded = await loadMappedRowsForPtrs({
        customerId,
        ptrsId,
        limit,
        transaction,
      });
      rowsIn = loaded.rows.length;
      stagedRows = await transform(loaded.rows);
      rowsOut = stagedRows.length;
    } else {
      const defaultDatasetId = await resolveDefaultDatasetId({
        customerId,
        ptrsId,
        userId,
        transaction,
        db,
      });
      await db.PtrsStageRow.destroy({
        where: { customerId, ptrsId, profileId },
        transaction,
      });

      let afterRowNo = null;
      while (true) {
        const loaded = await loadMappedRowsForPtrs({
          customerId,
          ptrsId,
          limit: PERSIST_BATCH_SIZE,
          afterRowNo,
          transaction,
        });
        if (!loaded.rows.length) break;
        rowsIn += loaded.rows.length;
        const batch = await transform(loaded.rows);
        const persistenceRows = batch.map((row) => {
          const data = buildPersistedStageRow(row, persistedStageFields);
          return {
            customerId,
            ptrsId,
            profileId,
            datasetId: row._dataset_id || defaultDatasetId,
            rowNo: row.row_no,
            ...buildStageColumnProjection(data, db.PtrsStageRow),
            data,
            errors: Array.isArray(row._stageErrors)
              ? row._stageErrors
              : null,
            meta: {
              ...(row._transformationMeta || {}),
              _stage: "ptrs.v2.stagePtrs",
              at: new Date().toISOString(),
              appliedRules: Array.isArray(row._appliedRules)
                ? row._appliedRules
                : [],
            },
            createdBy: userId || null,
            updatedBy: userId || null,
          };
        });
        await db.PtrsStageRow.bulkCreate(persistenceRows, {
          transaction,
          validate: true,
        });
        rowsOut += persistenceRows.length;
        afterRowNo = Number(loaded.rows.at(-1)?.row_no);
        if (loaded.rows.length < PERSIST_BATCH_SIZE) break;
      }
      persistedCount = rowsOut;
    }

    if (executionRun?.id) {
      await updateExecutionRun({
        customerId,
        executionRunId: executionRun.id,
        status: "success",
        finishedAt: new Date(),
        rowsIn,
        rowsOut,
        stats: totalStats,
        errorMessage: null,
        updatedBy: userId || null,
        transaction,
      });
    }
    trace?.write("stage_before_commit", {
      rowsIn,
      rowsOut,
      persistedCount,
      totalMs: hrMsSince(jobStartNs),
    });
    await transaction.commit();
    if (trace) await trace.close();
    return {
      rowsIn,
      rowsOut,
      persistedCount,
      tookMs: Date.now() - started,
      sample: persist ? null : stagedRows[0] || null,
      stats: totalStats,
    };
  } catch (error) {
    if (executionRun?.id && !transaction.finished) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "failed",
          finishedAt: new Date(),
          errorMessage: error?.message || "Stage failed",
          updatedBy: userId || null,
          transaction,
        });
      } catch (_) {
        // The original staging error remains authoritative.
      }
    }
    if (!transaction.finished) await transaction.rollback();
    trace?.write("stage_error", {
      message: error?.message || null,
      statusCode: error?.statusCode || null,
      totalMs: hrMsSince(jobStartNs),
    });
    if (trace) await trace.close();
    throw error;
  }
}

async function resolveDefaultDatasetId({
  customerId,
  ptrsId,
  userId,
  transaction,
  db,
}) {
  const datasets = await db.PtrsDataset.findAll({
    where: { customerId, ptrsId },
    attributes: ["id", "role", "updatedAt", "createdAt"],
    order: [
      ["updatedAt", "DESC"],
      ["createdAt", "DESC"],
    ],
    raw: true,
    transaction,
  });
  const normaliseRole = (role) => String(role || "").trim().toLowerCase();
  const main = datasets.find((row) => normaliseRole(row.role) === "main");
  const mainLike = datasets.find((row) =>
    normaliseRole(row.role).startsWith("main"),
  );
  if (main?.id || mainLike?.id) return main?.id || mainLike.id;

  const rawCount = await db.PtrsImportRaw.count({
    where: { customerId, ptrsId },
    transaction,
  });
  if (rawCount > 0) {
    const created = await db.PtrsDataset.create(
      {
        customerId,
        ptrsId,
        role: "main",
        fileName: "Main input",
        storageRef: null,
        rowsCount: rawCount,
        status: "uploaded",
        meta: { source: "raw", rowsCount: rawCount },
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction },
    );
    if (created?.id) return created.id;
  }
  const error = new Error(
    "No dataset is available for staging. Upload a dataset or complete an import first.",
  );
  error.statusCode = 400;
  throw error;
}

module.exports = {
  addStats,
  collectRuleOutputFields,
  collectConfiguredCanonicalFields,
  parseRowRules,
  stagePtrs,
  transformStageRows,
};
