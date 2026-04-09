function sanitizeForJsonbDeep(value) {
  if (value === undefined) return null;
  if (value === null) return null;

  if (typeof value === "string") {
    return value.includes("\u0000") ? value.replace(/\u0000/g, "") : value;
  }

  if (Array.isArray(value)) {
    return value.map((v) => sanitizeForJsonbDeep(v));
  }

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = sanitizeForJsonbDeep(v);
    }
    return out;
  }

  return value;
}

async function persistMappedRowsInBatches({
  customerId,
  ptrsId,
  actorId,
  transaction,
  trace,
  batchSize,
  maxHeaderKeys,
  composeMappedRowsForPtrs,
  ensureCanonicalRowShape,
  hrMsSince,
  db,
  slog,
  safeMeta,
}) {
  let totalPersisted = 0;
  let canonicalHeaders = [];
  const headersSet = new Set();
  const nowIso = new Date().toISOString();

  trace?.write("batch_begin", { limit: batchSize, offset: 0 });

  const composeStartNs = process.hrtime.bigint();
  const { rows, headers } = await composeMappedRowsForPtrs({
    customerId,
    ptrsId,
    transaction,
    trace,
  });
  trace?.write("batch_compose_end", {
    offset: 0,
    durationMs: hrMsSince(composeStartNs),
    rowsComposed: Array.isArray(rows) ? rows.length : 0,
  });

  if (Array.isArray(headers)) {
    canonicalHeaders = headers.slice(0, maxHeaderKeys);
    canonicalHeaders.forEach((h) => headersSet.add(h));
  }

  if (!rows || !rows.length) {
    return {
      totalPersisted,
      canonicalHeaders,
      hadRows: false,
    };
  }

  if (!canonicalHeaders.length) {
    for (const row of rows) {
      const canonicalRow = ensureCanonicalRowShape(row);
      for (const k of Object.keys(canonicalRow || {})) {
        if (headersSet.size < maxHeaderKeys) headersSet.add(k);
      }
    }
    canonicalHeaders = Array.from(headersSet);
  }

  for (let start = 0; start < rows.length; start += batchSize) {
    const batchStartNs = process.hrtime.bigint();
    const slice = rows.slice(start, start + batchSize);

    const payload = [];
    for (let i = 0; i < slice.length; ++i) {
      const row = slice[i];
      const canonicalRow = sanitizeForJsonbDeep(ensureCanonicalRowShape(row));
      payload.push({
        customerId,
        ptrsId,
        rowNo:
          typeof row.row_no === "number" && Number.isFinite(row.row_no)
            ? row.row_no
            : start + i + 1,
        data: canonicalRow,
        meta: sanitizeForJsonbDeep({
          stage: "ptrs.v2.mapped",
          builtAt: nowIso,
          builtBy: actorId || null,
          profileId: row?._ptrsMeta?.profileId || null,
          datasetId: row?._ptrsMeta?.datasetId || null,
        }),
      });
    }

    try {
      const persistStartNs = process.hrtime.bigint();
      trace?.write("batch_persist_begin", {
        offset: start,
        batchSize: payload.length,
      });
      await db.PtrsMappedRow.bulkCreate(payload, {
        transaction,
        validate: false,
      });
      trace?.write("batch_persist_end", {
        offset: start,
        batchSize: payload.length,
        durationMs: hrMsSince(persistStartNs),
      });
    } catch (e) {
      slog.error(
        "PTRS v2 buildMappedDatasetForPtrs: bulkCreate failed",
        safeMeta({
          customerId,
          ptrsId,
          offset: start,
          batchSize: payload.length,
          message: e?.message || null,
          name: e?.name || null,
          pgMessage: e?.parent?.message || e?.original?.message || null,
          pgDetail: e?.parent?.detail || null,
          pgCode: e?.parent?.code || null,
          errors: Array.isArray(e?.errors)
            ? e.errors.map((x) => ({ message: x?.message, path: x?.path }))
            : null,
        }),
      );
      throw e;
    }

    totalPersisted += payload.length;

    trace?.write("batch_end", {
      offset: start,
      persistedSoFar: totalPersisted,
      durationMs: hrMsSince(batchStartNs),
    });
  }

  return {
    totalPersisted,
    canonicalHeaders,
    hadRows: true,
  };
}

async function buildMappedDatasetForPtrs({
  customerId,
  ptrsId,
  profileId = null,
  actorId = null,
  beginTransactionWithCustomerContext,
  createPtrsTrace,
  hrMsSince,
  safeMeta,
  slog,
  getMapStaleness,
  createExecutionRun,
  updateExecutionRun,
  getMapCompletionGate,
  persistMappedRowsInBatches,
  composeMappedRowsForPtrs,
  ensureCanonicalRowShape,
  db,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const BATCH_SIZE = 2000;
  const MAX_HEADER_KEYS = 2000;

  const t = await beginTransactionWithCustomerContext(customerId);

  slog.info("PTRS_TRACE maps", {
    PTRS_TRACE: process.env.PTRS_TRACE,
    PTRS_TRACE_DIR: process.env.PTRS_TRACE_DIR,
  });

  const trace = createPtrsTrace({
    customerId,
    ptrsId,
    actorId,
    logInfo: (msg, meta) => slog.info(msg, meta),
    meta: safeMeta,
  });
  const jobStartNs = process.hrtime.bigint();
  trace?.write("build_begin", { batchSize: BATCH_SIZE });
  let executionRun = null;

  const sStaleness = process.hrtime.bigint();
  const staleness = await getMapStaleness({
    customerId,
    ptrsId,
    profileId,
    transaction: t,
  });
  trace?.write("build_staleness_checked", {
    durationMs: hrMsSince(sStaleness),
    profileId: profileId || staleness?.snapshot?.profileId || null,
    hasChanged: !!staleness?.hasChanged,
    existingMappedRowCount: Number(staleness?.existingMappedRowCount) || 0,
    previousRunId: staleness?.previousRunId || null,
    inputHash: staleness?.inputHash || null,
  });

  executionRun = await createExecutionRun({
    customerId,
    ptrsId,
    profileId: profileId || staleness?.snapshot?.profileId || null,
    step: "map",
    inputHash: staleness?.inputHash || null,
    status: "running",
    startedAt: new Date(),
    createdBy: actorId || null,
    transaction: t,
  });

  if (!staleness?.hasChanged && Number(staleness?.existingMappedRowCount) > 0) {
    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: inputs unchanged; skipping combined-row materialisation",
      safeMeta({
        customerId,
        ptrsId,
        inputHash: staleness?.inputHash || null,
        previousRunId: staleness?.previousRunId || null,
        existingMappedRowCount: Number(staleness?.existingMappedRowCount) || 0,
      }),
    );

    trace?.write("build_skip_input_unchanged", {
      inputHash: staleness?.inputHash || null,
      previousRunId: staleness?.previousRunId || null,
      existingMappedRowCount: Number(staleness?.existingMappedRowCount) || 0,
      totalMs: hrMsSince(jobStartNs),
    });

    const gate = await getMapCompletionGate({
      customerId,
      ptrsId,
      profileId: profileId || staleness?.snapshot?.profileId || null,
      transaction: t,
    });

    if (executionRun?.id) {
      await updateExecutionRun({
        customerId,
        executionRunId: executionRun.id,
        status: "success",
        finishedAt: new Date(),
        rowsIn: Number(staleness?.existingMappedRowCount) || 0,
        rowsOut: Number(staleness?.existingMappedRowCount) || 0,
        stats: {
          skipped: true,
          reason: "INPUT_UNCHANGED",
          gate: gate?.summary || null,
        },
        errorMessage: null,
        updatedBy: actorId || null,
        transaction: t,
      });
    }

    await t.commit();
    if (trace) await trace.close();
    return {
      skipped: true,
      reason: "INPUT_UNCHANGED",
      inputHash: staleness?.inputHash || null,
      previousRunId: staleness?.previousRunId || null,
      count: Number(staleness?.existingMappedRowCount) || 0,
      headers: [],
      gate,
    };
  }

  try {
    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: begin combined-row materialisation",
      safeMeta({ customerId, ptrsId }),
    );

    const destroyStartNs = process.hrtime.bigint();
    trace?.write("mapped_rows_destroy_begin");

    const deleteResult = await db.sequelize.query(
      `
      DELETE FROM "tbl_ptrs_mapped_row"
      WHERE "customerId" = :customerId
        AND "ptrsId" = :ptrsId
      `,
      {
        replacements: { customerId, ptrsId },
        transaction: t,
      },
    );

    trace?.write("mapped_rows_destroy_end", {
      durationMs: hrMsSince(destroyStartNs),
      deleteResult: Array.isArray(deleteResult)
        ? deleteResult[1] || null
        : null,
    });

    const { totalPersisted, canonicalHeaders, hadRows } =
      await persistMappedRowsInBatches({
        customerId,
        ptrsId,
        actorId,
        transaction: t,
        trace,
        batchSize: BATCH_SIZE,
        maxHeaderKeys: MAX_HEADER_KEYS,
        composeMappedRowsForPtrs,
        ensureCanonicalRowShape,
        hrMsSince,
        db,
        slog,
        safeMeta,
      });

    if (!hadRows) {
      trace?.write("build_no_rows", { totalMs: hrMsSince(jobStartNs) });
      slog.info(
        "PTRS v2 buildMappedDatasetForPtrs: no rows composed, nothing persisted",
        safeMeta({ customerId, ptrsId }),
      );

      const gate = await getMapCompletionGate({
        customerId,
        ptrsId,
        profileId: profileId || staleness?.snapshot?.profileId || null,
        transaction: t,
      });

      if (executionRun?.id) {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "success",
          finishedAt: new Date(),
          rowsIn: 0,
          rowsOut: 0,
          stats: {
            headersCount: 0,
            reason: "NO_ROWS_COMPOSED",
            gate: gate?.summary || null,
          },
          errorMessage: null,
          updatedBy: actorId || null,
          transaction: t,
        });
      }

      await t.commit();
      if (trace) await trace.close();
      return { count: 0, headers: [], gate };
    }

    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: persisted mapped rows",
      safeMeta({
        customerId,
        ptrsId,
        rowsPersisted: totalPersisted,
        headersCount: Array.isArray(canonicalHeaders)
          ? canonicalHeaders.length
          : 0,
      }),
    );

    const gate = await getMapCompletionGate({
      customerId,
      ptrsId,
      profileId: profileId || staleness?.snapshot?.profileId || null,
      transaction: t,
    });

    if (executionRun?.id) {
      await updateExecutionRun({
        customerId,
        executionRunId: executionRun.id,
        status: "success",
        finishedAt: new Date(),
        rowsIn: totalPersisted,
        rowsOut: totalPersisted,
        stats: {
          headersCount: Array.isArray(canonicalHeaders)
            ? canonicalHeaders.length
            : 0,
          gate: gate?.summary || null,
        },
        errorMessage: null,
        updatedBy: actorId || null,
        transaction: t,
      });
    }
    trace?.write("build_before_commit", {
      rowsPersisted: totalPersisted,
      headersCount: Array.isArray(canonicalHeaders)
        ? canonicalHeaders.length
        : 0,
      totalMs: hrMsSince(jobStartNs),
    });

    await t.commit();

    trace?.write("build_committed", { totalMs: hrMsSince(jobStartNs) });
    if (trace) await trace.close();

    return {
      count: totalPersisted,
      headers: canonicalHeaders || [],
      gate,
    };
  } catch (err) {
    trace?.write("build_error", {
      message: err?.message || null,
      statusCode: err?.statusCode || null,
      totalMs: hrMsSince(jobStartNs),
    });
    if (executionRun?.id) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "failed",
          finishedAt: new Date(),
          errorMessage: err?.message || "Map build failed",
          updatedBy: actorId || null,
          transaction: t,
        });
      } catch (_) {}
    }
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    if (trace) await trace.close();
    throw err;
  }
}

module.exports = {
  sanitizeForJsonbDeep,
  persistMappedRowsInBatches,
  buildMappedDatasetForPtrs,
};
