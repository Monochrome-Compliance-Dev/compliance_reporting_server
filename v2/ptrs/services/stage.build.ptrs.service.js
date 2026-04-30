async function stagePtrs({
  customerId,
  ptrsId,
  persist = false,
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
  db,
}) {
  let executionRun = null;
  let inputHash = null;

  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  if (!persist) {
    const e = new Error(
      "stagePtrs requires persist=true; non-persist staging has been removed",
    );
    e.statusCode = 400;
    throw e;
  }

  if (!profileId) {
    const e = new Error("profileId is required when persist=true");
    e.statusCode = 400;
    throw e;
  }

  const started = Date.now();
  const t = await beginTransactionWithCustomerContext(customerId);

  const trace = createPtrsTrace({
    customerId,
    ptrsId,
    actorId: userId || null,
    logInfo: (msg, meta) => slog.info(msg, meta),
    meta: safeMeta,
  });

  const jobStartNs = process.hrtime.bigint();
  const stageStart = (name) => ({ name, startNs: process.hrtime.bigint() });
  const stageEnd = (s, extra = {}) => {
    if (!s) return;
    trace?.write("stage_stage_end", {
      stage: s.name,
      durationMs: hrMsSince(s.startNs),
      ...extra,
    });
  };

  trace?.write("stage_begin", {
    persist: true,
    force: !!force,
    hasProfileId: !!profileId,
  });

  try {
    const staleness = await getStageStaleness({
      customerId,
      ptrsId,
      profileId,
      transaction: t,
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
          transaction: t,
        });

    if (
      !force &&
      previous &&
      previous.status === "success" &&
      previous.inputHash === inputHash
    ) {
      const existingStageCount = Number(staleness?.existingStageCount) || 0;

      if (Number(existingStageCount) > 0) {
        slog.info(
          "PTRS v2 stagePtrs: inputs unchanged; skipping persist staging",
          {
            action: "PtrsV2StagePtrsSkipped",
            customerId,
            ptrsId,
            profileId: profileId || null,
            inputHash,
            previousRunId: previous.id || null,
            existingStageCount,
          },
        );
        trace?.write("stage_skip_input_unchanged", {
          inputHash,
          previousRunId: previous.id || null,
          existingStageCount,
          totalMs: hrMsSince(jobStartNs),
        });

        await t.commit();
        if (trace) await trace.close();
        return {
          skipped: true,
          reason: "INPUT_UNCHANGED",
          inputHash,
          previousRunId: previous.id || null,
          persistedCount: existingStageCount,
          rowsIn: null,
          rowsOut: null,
          tookMs: Date.now() - started,
          sample: null,
          stats: null,
        };
      }

      slog.info(
        "PTRS v2 stagePtrs: inputs unchanged but no staged rows exist; forcing rebuild",
        {
          action: "PtrsV2StagePtrsSkipBypassedNoRows",
          customerId,
          ptrsId,
          profileId: profileId || null,
          inputHash,
          previousRunId: previous.id || null,
          existingStageCount,
        },
      );
      trace?.write("stage_skip_bypassed_no_rows", {
        inputHash,
        previousRunId: previous.id || null,
        existingStageCount,
        totalMs: hrMsSince(jobStartNs),
      });
    }

    slog.info("PTRS v2 stagePtrs: execution input hash", {
      action: "PtrsV2StagePtrsInputHash",
      customerId,
      ptrsId,
      profileId: profileId || null,
      inputHash,
      previousHash: previous?.inputHash || null,
      mappedRowCount: staleness?.snapshot?.mappedRows?.rowCount || 0,
      mappedRowUpdatedAt: staleness?.snapshot?.mappedRows?.maxUpdatedAt || null,
      datasetsCount: Array.isArray(staleness?.snapshot?.datasets)
        ? staleness.snapshot.datasets.length
        : 0,
      paymentTermMapCount:
        Number(staleness?.snapshot?.paymentTermMap?.count) || 0,
      paymentTermMapUpdatedAt:
        staleness?.snapshot?.paymentTermMap?.maxUpdatedAt || null,
      paymentTermChangeCount:
        Number(staleness?.snapshot?.paymentTermChanges?.count) || 0,
      paymentTermChangeUpdatedAt:
        staleness?.snapshot?.paymentTermChanges?.maxUpdatedAt || null,
    });

    executionRun = await createExecutionRun({
      customerId,
      ptrsId,
      profileId,
      step: "stage",
      inputHash,
      status: "running",
      startedAt: new Date(),
      createdBy: userId || null,
      transaction: t,
    });

    const rulesStats = null;
    const paymentTermChangeStats = null;
    const paymentTermStats = null;

    let persistedCount = null;
    let persistedRowsIn = null;
    let persistedRowsOut = null;

    const resolveDefaultDatasetId = async () => {
      if (!db.PtrsDataset) {
        const e = new Error(
          "PtrsDataset model is not available; cannot resolve datasetId for staging",
        );
        e.statusCode = 500;
        throw e;
      }

      const all = await db.PtrsDataset.findAll({
        where: { customerId, ptrsId },
        attributes: ["id", "role", "updatedAt", "createdAt", "storageRef"],
        order: [
          ["updatedAt", "DESC"],
          ["createdAt", "DESC"],
        ],
        transaction: t,
        raw: true,
      });

      const normRole = (r) =>
        String(r || "")
          .trim()
          .toLowerCase();
      const main = all.find((d) => normRole(d.role) === "main");
      if (main?.id) return main.id;

      const mainLike = all.find((d) => normRole(d.role).startsWith("main"));
      if (mainLike?.id) return mainLike.id;

      const rawCount = await db.PtrsImportRaw.count({
        where: { customerId, ptrsId },
        transaction: t,
      });

      if (rawCount && rawCount > 0) {
        const candidate = {
          customerId,
          ptrsId,
          role: "main",
          fileName: "Main input",
          storageRef: null,
          rowsCount: rawCount,
          status: "uploaded",
          meta: {
            source: "raw",
            rowsCount: rawCount,
            displayName: "Main input",
            updatedAt: new Date().toISOString(),
          },
          createdBy: userId || null,
          updatedBy: userId || null,
        };

        const created = await db.PtrsDataset.create(candidate, {
          transaction: t,
        });

        if (created?.id) return created.id;
      }

      const e = new Error(
        "No dataset is available for staging. Upload a dataset or complete an import first.",
      );
      e.statusCode = 400;
      throw e;
    };

    const defaultDatasetId = await resolveDefaultDatasetId();

    if (!defaultDatasetId) {
      const e = new Error("Unable to resolve datasetId for staging");
      e.statusCode = 500;
      throw e;
    }

    const sPersistClear = stageStart("persist_stage_clear");
    const clearSql = `
        DELETE FROM "tbl_ptrs_stage_row"
        WHERE "customerId" = :customerId
          AND "ptrsId" = :ptrsId
          AND "profileId" = :profileId
      `;
    const [, clearMeta] = await db.sequelize.query(clearSql, {
      transaction: t,
      replacements: { customerId, ptrsId, profileId },
    });
    const clearedCount = Number(clearMeta?.rowCount || clearMeta || 0);
    stageEnd(sPersistClear, { clearedCount });

    slog.info("PTRS v2 stagePtrs: cleared active stage rows", {
      action: "PtrsV2StagePtrsClearActive",
      customerId,
      ptrsId,
      clearedCount: Number(clearedCount) || 0,
    });

    const insertSql = `
        WITH source_rows AS (
          SELECT
            m."rowNo",
            COALESCE(m."data", '{}'::jsonb) AS "data",
            COALESCE(
              NULLIF(m."meta"->>'datasetId', ''),
              NULLIF(m."data"->'_ptrsMeta'->>'datasetId', ''),
              :defaultDatasetId
            ) AS "datasetId",
            NULLIF(m."data"->>'payer_entity_name', '') AS "payerEntityName",
            NULLIF(regexp_replace(COALESCE(m."data"->>'payer_entity_abn', ''), '[^0-9]', '', 'g'), '') AS "payerEntityAbn",
            NULLIF(m."data"->>'payee_entity_name', '') AS "payeeEntityName",
            NULLIF(regexp_replace(COALESCE(m."data"->>'payee_entity_abn', ''), '[^0-9]', '', 'g'), '') AS "payeeEntityAbn",
            NULLIF(regexp_replace(COALESCE(m."data"->>'payee_entity_abn', ''), '[^0-9]', '', 'g'), '') AS "payeeEntityAbnDigits",
            NULLIF(m."data"->>'invoice_reference_number', '') AS "invoiceReferenceNumber",
            NULLIF(m."data"->>'source_account_code', '') AS "sourceAccountCode",
            NULLIF(m."data"->>'description', '') AS "description",
            NULLIF(m."data"->>'document_type', '') AS "documentType",
            NULLIF(m."data"->>'document_currency', '') AS "documentCurrency",
            NULLIF(m."data"->>'clearing_document', '') AS "clearingDocument",
            NULLIF(m."data"->>'reconciliation_status', '') AS "reconciliationStatus",
            NULLIF(m."data"->>'source_user', '') AS "sourceUser",
            NULLIF(REPLACE(COALESCE(m."data"->>'payment_amount', ''), ',', ''), '') AS "paymentAmountRaw",
            NULLIF(m."data"->>'payment_date', '') AS "paymentDateRaw",
            NULLIF(m."data"->>'invoice_issue_date', '') AS "invoiceIssueDateRaw",
            NULLIF(m."data"->>'invoice_created_date', '') AS "invoiceCreatedDateRaw",
            NULLIF(m."data"->>'entry_date', '') AS "entryDateRaw",
            NULLIF(m."data"->>'invoice_receipt_date', '') AS "invoiceReceiptDateRaw",
            NULLIF(m."data"->>'invoice_due_date', '') AS "invoiceDueDateRaw",
            NULLIF(m."data"->>'invoice_payment_terms', '') AS "paymentTermRaw",
            NULLIF(m."data"->>'payment_term_days', '') AS "paymentTermDaysRaw"
          FROM "tbl_ptrs_mapped_row" m
          WHERE m."customerId" = :customerId
            AND m."ptrsId" = :ptrsId
        ),
        parsed_rows AS (
          SELECT
            sr.*,
            CASE
              WHEN sr."paymentAmountRaw" IS NULL THEN NULL
              ELSE ABS(sr."paymentAmountRaw"::numeric)
            END AS "paymentAmount",
            CASE
              WHEN sr."paymentDateRaw" IS NULL THEN NULL
              WHEN sr."paymentDateRaw" ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN sr."paymentDateRaw"::date
              WHEN sr."paymentDateRaw" ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(sr."paymentDateRaw", 'DD/MM/YYYY')
              ELSE NULL
            END AS "paymentDate",
            CASE
              WHEN sr."invoiceIssueDateRaw" IS NULL THEN NULL
              WHEN sr."invoiceIssueDateRaw" ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN sr."invoiceIssueDateRaw"::date
              WHEN sr."invoiceIssueDateRaw" ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(sr."invoiceIssueDateRaw", 'DD/MM/YYYY')
              ELSE NULL
            END AS "invoiceIssueDate",
            CASE
              WHEN sr."invoiceCreatedDateRaw" IS NULL THEN NULL
              WHEN sr."invoiceCreatedDateRaw" ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN sr."invoiceCreatedDateRaw"::date
              WHEN sr."invoiceCreatedDateRaw" ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(sr."invoiceCreatedDateRaw", 'DD/MM/YYYY')
              ELSE NULL
            END AS "invoiceCreatedDate",
            CASE
              WHEN sr."entryDateRaw" IS NULL THEN NULL
              WHEN sr."entryDateRaw" ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN sr."entryDateRaw"::date
              WHEN sr."entryDateRaw" ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(sr."entryDateRaw", 'DD/MM/YYYY')
              ELSE NULL
            END AS "entryDate",
            CASE
              WHEN sr."invoiceReceiptDateRaw" IS NULL THEN NULL
              WHEN sr."invoiceReceiptDateRaw" ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN sr."invoiceReceiptDateRaw"::date
              WHEN sr."invoiceReceiptDateRaw" ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(sr."invoiceReceiptDateRaw", 'DD/MM/YYYY')
              ELSE NULL
            END AS "invoiceReceiptDateMapped",
            CASE
              WHEN sr."invoiceDueDateRaw" IS NULL THEN NULL
              WHEN sr."invoiceDueDateRaw" ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN sr."invoiceDueDateRaw"::date
              WHEN sr."invoiceDueDateRaw" ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(sr."invoiceDueDateRaw", 'DD/MM/YYYY')
              ELSE NULL
            END AS "invoiceDueDate",
            CASE
              WHEN sr."paymentTermDaysRaw" IS NULL THEN NULL
              ELSE sr."paymentTermDaysRaw"::int
            END AS "paymentTermDays",
            CASE
              WHEN sr."payeeEntityAbnDigits" !~ '^\\d{11}$' THEN false
              ELSE (
                (
                  ((SUBSTRING(sr."payeeEntityAbnDigits", 1, 1)::int - 1) * 10) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 2, 1)::int * 1) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 3, 1)::int * 3) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 4, 1)::int * 5) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 5, 1)::int * 7) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 6, 1)::int * 9) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 7, 1)::int * 11) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 8, 1)::int * 13) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 9, 1)::int * 15) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 10, 1)::int * 17) +
                  (SUBSTRING(sr."payeeEntityAbnDigits", 11, 1)::int * 19)
                ) % 89 = 0
              )
            END AS "payeeEntityAbnValid"
          FROM source_rows sr
        )
        INSERT INTO "tbl_ptrs_stage_row"
        (
          "id",
          "customerId",
          "ptrsId",
          "profileId",
          "datasetId",
          "rowNo",
          "payerEntityName",
          "payerEntityAbn",
          "payeeEntityName",
          "payeeEntityAbn",
          "payeeEntityAbnValid",
          "invoiceReferenceNumber",
          "sourceAccountCode",
          "description",
          "documentType",
          "documentCurrency",
          "clearingDocument",
          "reconciliationStatus",
          "sourceUser",
          "paymentAmount",
          "paymentDate",
          "invoiceIssueDate",
          "invoiceReceiptDate",
          "invoiceDueDate",
          "invoiceCreatedDate",
          "entryDate",
          "paymentTermRaw",
          "paymentTermDays",
          "paymentTimeDays",
          "data",
          "errors",
          "meta",
          "createdBy",
          "updatedBy",
          "createdAt",
          "updatedAt"
        )
        SELECT
          SUBSTRING(
            md5(
              :customerId || ':' ||
              :ptrsId || ':' ||
              COALESCE(:profileId, '') || ':' ||
              pr."datasetId" || ':' ||
              pr."rowNo"::text
            ),
            1,
            10
          ) AS "id",
          :customerId AS "customerId",
          :ptrsId AS "ptrsId",
          :profileId AS "profileId",
          pr."datasetId",
          pr."rowNo",
          pr."payerEntityName",
          pr."payerEntityAbn",
          pr."payeeEntityName",
          pr."payeeEntityAbn",
          pr."payeeEntityAbnValid",
          pr."invoiceReferenceNumber",
          pr."sourceAccountCode",
          pr."description",
          pr."documentType",
          pr."documentCurrency",
          pr."clearingDocument",
          pr."reconciliationStatus",
          pr."sourceUser",
          pr."paymentAmount",
          pr."paymentDate",
          pr."invoiceIssueDate",
          COALESCE(
            pr."invoiceCreatedDate",
            pr."entryDate",
            pr."invoiceReceiptDateMapped"
          ) AS "invoiceReceiptDate",
          pr."invoiceDueDate",
          pr."invoiceCreatedDate",
          pr."entryDate",
          pr."paymentTermRaw",
          CASE
            WHEN pr."paymentTermRaw" ~ '^\\d{1,4}$' THEN pr."paymentTermRaw"::int
            ELSE ptm."transformedDays"
          END AS "paymentTermDays",
          CASE
            WHEN pr."paymentDate" IS NULL THEN NULL
            WHEN pr."invoiceIssueDate" IS NOT NULL
              AND COALESCE(
                pr."invoiceCreatedDate",
                pr."entryDate",
                pr."invoiceReceiptDateMapped"
              ) IS NOT NULL THEN
              GREATEST(
                0,
                LEAST(
                  pr."paymentDate" - pr."invoiceIssueDate",
                  pr."paymentDate" - COALESCE(
                    pr."invoiceCreatedDate",
                    pr."entryDate",
                    pr."invoiceReceiptDateMapped"
                  )
                )
              ) + 1
            WHEN COALESCE(
              pr."invoiceCreatedDate",
              pr."entryDate",
              pr."invoiceReceiptDateMapped"
            ) IS NOT NULL THEN
              GREATEST(
                0,
                pr."paymentDate" - COALESCE(
                  pr."invoiceCreatedDate",
                  pr."entryDate",
                  pr."invoiceReceiptDateMapped"
                )
              ) + 1
            WHEN pr."invoiceIssueDate" IS NOT NULL THEN
              GREATEST(
                0,
                pr."paymentDate" - pr."invoiceIssueDate"
              ) + 1
            WHEN pr."invoiceDueDate" IS NOT NULL THEN
              GREATEST(
                0,
                pr."paymentDate" - pr."invoiceDueDate"
              ) + 1
            ELSE NULL
          END AS "paymentTimeDays",
          pr."data",
          NULL AS "errors",
          jsonb_build_object(
            '_stage', 'ptrs.v2.stagePtrs',
            'at', NOW(),
            'rules', to_jsonb(:rulesStats::json)
          ) AS "meta",
          :userId AS "createdBy",
          :userId AS "updatedBy",
          NOW() AS "createdAt",
          NOW() AS "updatedAt"
        FROM parsed_rows pr
        LEFT JOIN "tbl_ptrs_payment_term_map" ptm
          ON ptm."customerId" = :customerId
         AND ptm."profileId" = :profileId
         AND ptm."deletedAt" IS NULL
         AND ptm."raw" = pr."paymentTermRaw"
      `;

    slog.info("PTRS v2 stagePtrs: preparing SQL insert", {
      action: "PtrsV2StagePtrsSqlInsert",
      customerId,
      ptrsId,
      defaultDatasetId,
    });

    try {
      const sPersistInsert = stageStart("persist_stage_insert_select");
      const [, insertMeta] = await db.sequelize.query(insertSql, {
        transaction: t,
        replacements: {
          customerId,
          ptrsId,
          profileId,
          defaultDatasetId,
          userId: userId || null,
          rulesStats: JSON.stringify(rulesStats || null),
        },
      });

      persistedRowsOut = Number(insertMeta?.rowCount || insertMeta || 0);
      persistedRowsIn = persistedRowsOut;
      persistedCount = persistedRowsOut;
      stageEnd(sPersistInsert, { inserted: persistedRowsOut });
    } catch (err) {
      trace?.write("stage_persist_insert_select_error", {
        message: err?.message || null,
        statusCode: err?.statusCode || null,
      });
      slog.error("PTRS v2 stagePtrs: SQL insert-select failed", {
        action: "PtrsV2StagePtrsInsertSelectFailed",
        customerId,
        ptrsId,
        error: err?.message,
      });
      throw err;
    }

    const tookMs = Date.now() - started;

    if (executionRun?.id) {
      try {
        const sRunUpdate = stageStart("execution_run_update_success");
        const effectiveRowsIn = Number(persistedRowsIn || 0);
        const effectiveRowsOut = Number(persistedRowsOut || 0);

        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "success",
          finishedAt: new Date(),
          rowsIn: effectiveRowsIn,
          rowsOut: effectiveRowsOut,
          stats: {
            rules: rulesStats,
            paymentTerms: paymentTermStats,
            paymentTermChanges: paymentTermChangeStats,
          },
          errorMessage: null,
          updatedBy: userId || null,
          transaction: t,
        });
        stageEnd(sRunUpdate, { executionRunId: executionRun.id || null });
      } catch (e) {
        trace?.write("execution_run_update_error", {
          executionRunId: executionRun.id || null,
          message: e?.message || null,
        });
        slog.warn(
          "PTRS v2 stagePtrs: failed to update execution run (non-fatal)",
          {
            action: "PtrsV2StagePtrsUpdateExecutionRunFailed",
            customerId,
            ptrsId,
            executionRunId: executionRun.id,
            error: e?.message,
          },
        );
      }
    }

    const effectiveRowsIn = Number(persistedRowsIn || 0);
    const effectiveRowsOut = Number(persistedRowsOut || 0);

    trace?.write("stage_before_commit", {
      rowsIn: effectiveRowsIn,
      rowsOut: effectiveRowsOut,
      persistedCount,
      totalMs: hrMsSince(jobStartNs),
      tookMs: Date.now() - started,
    });

    await t.commit();
    trace?.write("stage_committed", { totalMs: hrMsSince(jobStartNs) });
    if (trace) await trace.close();

    return {
      rowsIn: Number(persistedRowsIn || 0),
      rowsOut: Number(persistedRowsOut || 0),
      persistedCount,
      tookMs,
      sample: null,
      stats: {
        rules: rulesStats,
        paymentTerms: paymentTermStats,
        paymentTermChanges: paymentTermChangeStats,
      },
    };
  } catch (err) {
    trace?.write("stage_error", {
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
          errorMessage: err?.message || "Stage failed",
          updatedBy: userId || null,
          transaction: t,
        });
      } catch (_) {
        // best-effort only
      }
    }

    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {
        // ignore rollback errors
      }
    }
    if (trace) await trace.close();
    throw err;
  }
}

module.exports = {
  stagePtrs,
};
