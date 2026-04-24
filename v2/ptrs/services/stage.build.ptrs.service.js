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
    const clearedCount = await db.PtrsStageRow.destroy({
      where: { customerId, ptrsId, profileId },
      transaction: t,
    });
    stageEnd(sPersistClear, { clearedCount: Number(clearedCount) || 0 });

    slog.info("PTRS v2 stagePtrs: cleared active stage rows", {
      action: "PtrsV2StagePtrsClearActive",
      customerId,
      ptrsId,
      clearedCount: Number(clearedCount) || 0,
    });

    const insertSql = `
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
          "invoiceReferenceNumber",
          "sourceAccountCode",
          "description",
          "documentType",
          "clearingDocument",
          "paymentAmount",
          "paymentDate",
          "invoiceIssueDate",
          "invoiceReceiptDate",
          "invoiceDueDate",
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
              m."rowNo"::text || ':' ||
              clock_timestamp()::text || ':' ||
              random()::text
            ),
            1,
            10
          ) AS "id",
          :customerId AS "customerId",
          :ptrsId AS "ptrsId",
          :profileId AS "profileId",
          COALESCE(
            NULLIF(m."meta"->>'datasetId', ''),
            NULLIF(m."data"->'_ptrsMeta'->>'datasetId', ''),
            :defaultDatasetId
          ) AS "datasetId",
          m."rowNo" AS "rowNo",
                    NULLIF(m."data"->>'payer_entity_name', '') AS "payerEntityName",
          NULLIF(regexp_replace(COALESCE(m."data"->>'payer_entity_abn', ''), '[^0-9]', '', 'g'), '') AS "payerEntityAbn",
          NULLIF(m."data"->>'payee_entity_name', '') AS "payeeEntityName",
          NULLIF(regexp_replace(COALESCE(m."data"->>'payee_entity_abn', ''), '[^0-9]', '', 'g'), '') AS "payeeEntityAbn",
          COALESCE(
            NULLIF(m."data"->>'invoice_reference_number', ''),
            NULLIF(m."data"->>'Invoice Reference', ''),
            NULLIF(m."data"->>'Document Number', '')
          ) AS "invoiceReferenceNumber",
          COALESCE(
            NULLIF(m."data"->>'source_account_code', ''),
            NULLIF(m."data"->>'Account', '')
          ) AS "sourceAccountCode",
          COALESCE(
            NULLIF(m."data"->>'description', ''),
            NULLIF(m."data"->>'Text', ''),
            NULLIF(m."data"->>'Reference', '')
          ) AS "description",
          COALESCE(
            NULLIF(m."data"->>'document_type', ''),
            NULLIF(m."data"->>'Document Type', '')
          ) AS "documentType",
          COALESCE(
            NULLIF(m."data"->>'clearing_document', ''),
            NULLIF(m."data"->>'Clearing Document', '')
          ) AS "clearingDocument",
          CASE
            WHEN NULLIF(REPLACE(COALESCE(m."data"->>'payment_amount', ''), ',', ''), '') IS NULL THEN NULL
            ELSE ABS(REPLACE(m."data"->>'payment_amount', ',', '')::numeric)
          END AS "paymentAmount",
          CASE
            WHEN NULLIF(m."data"->>'payment_date', '') IS NULL THEN NULL
            WHEN (m."data"->>'payment_date') ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (m."data"->>'payment_date')::date
            WHEN (m."data"->>'payment_date') ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(m."data"->>'payment_date', 'DD/MM/YYYY')
            ELSE NULL
          END AS "paymentDate",
          CASE
            WHEN NULLIF(m."data"->>'invoice_issue_date', '') IS NULL THEN NULL
            WHEN (m."data"->>'invoice_issue_date') ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (m."data"->>'invoice_issue_date')::date
            WHEN (m."data"->>'invoice_issue_date') ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(m."data"->>'invoice_issue_date', 'DD/MM/YYYY')
            ELSE NULL
          END AS "invoiceIssueDate",
          CASE
            WHEN NULLIF(m."data"->>'invoice_receipt_date', '') IS NULL THEN NULL
            WHEN (m."data"->>'invoice_receipt_date') ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (m."data"->>'invoice_receipt_date')::date
            WHEN (m."data"->>'invoice_receipt_date') ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(m."data"->>'invoice_receipt_date', 'DD/MM/YYYY')
            ELSE NULL
          END AS "invoiceReceiptDate",
          CASE
            WHEN NULLIF(m."data"->>'invoice_due_date', '') IS NULL THEN NULL
            WHEN (m."data"->>'invoice_due_date') ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (m."data"->>'invoice_due_date')::date
            WHEN (m."data"->>'invoice_due_date') ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(m."data"->>'invoice_due_date', 'DD/MM/YYYY')
            ELSE NULL
          END AS "invoiceDueDate",
          COALESCE(
            NULLIF(m."data"->>'payment_term', ''),
            NULLIF(m."data"->>'Payment terms', ''),
            NULLIF(m."data"->>'vendormaster__Payment terms', '')
          ) AS "paymentTermRaw",
          CASE
            WHEN NULLIF(m."data"->>'payment_term_days', '') IS NULL THEN NULL
            ELSE (m."data"->>'payment_term_days')::int
          END AS "paymentTermDays",
          CASE
            WHEN NULLIF(m."data"->>'payment_time_days', '') IS NULL THEN NULL
            ELSE (m."data"->>'payment_time_days')::int
          END AS "paymentTimeDays",
          COALESCE(m."data", '{}'::jsonb) AS "data",
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
        FROM "tbl_ptrs_mapped_row" m
        WHERE m."customerId" = :customerId
          AND m."ptrsId" = :ptrsId
        ORDER BY m."rowNo"
      `;

    slog.info("PTRS v2 stagePtrs: preparing SQL insert", {
      action: "PtrsV2StagePtrsSqlInsert",
      customerId,
      ptrsId,
      defaultDatasetId,
    });

    try {
      const countMappedSql = `
          SELECT COUNT(*)::int AS "count"
          FROM "tbl_ptrs_mapped_row" m
          WHERE m."customerId" = :customerId
            AND m."ptrsId" = :ptrsId
        `;

      const countStageSql = `
          SELECT COUNT(*)::int AS "count"
          FROM "tbl_ptrs_stage_row" s
          WHERE s."customerId" = :customerId
            AND s."ptrsId" = :ptrsId
            AND s."profileId" = :profileId
            AND s."deletedAt" IS NULL
        `;

      const sPersistCountMapped = stageStart("persist_stage_count_mapped_rows");
      const [mappedCountRows] = await db.sequelize.query(countMappedSql, {
        transaction: t,
        replacements: {
          customerId,
          ptrsId,
        },
      });
      persistedRowsIn = Number(mappedCountRows?.[0]?.count || 0);
      stageEnd(sPersistCountMapped, {
        mappedRowsIn: persistedRowsIn,
      });

      const sPersistInsert = stageStart("persist_stage_insert_select");
      await db.sequelize.query(insertSql, {
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

      const sPersistCountStage = stageStart("persist_stage_count_active_rows");
      const [stageCountRows] = await db.sequelize.query(countStageSql, {
        transaction: t,
        replacements: {
          customerId,
          ptrsId,
          profileId,
        },
      });
      persistedRowsOut = Number(stageCountRows?.[0]?.count || 0);
      persistedCount = persistedRowsOut;
      stageEnd(sPersistCountStage, {
        activeStageRows: persistedRowsOut,
      });
      stageEnd(sPersistInsert, { inserted: persistedRowsOut });

      const sPaymentTermUpdate = stageStart(
        "persist_stage_update_payment_term_days",
      );
      const paymentTermSql = `
          WITH term_source AS (
            SELECT
              s."id",
              NULLIF(
                TRIM(
                  COALESCE(
                    s."data"->>'payment_term_days',
                    s."data"->>'invoice_payment_terms_effective',
                    s."data"->>'invoice_payment_terms_raw',
                    s."data"->>'invoice_payment_terms',
                    s."data"->>'payment_term',
                    s."data"->>'vendormaster__Payment terms',
                    s."data"->>'default_payment_term',
                    s."data"->>'contract_po_payment_terms_effective',
                    s."data"->>'contract_po_payment_terms'
                  )
                ),
                ''
              ) AS "termCode"
            FROM "tbl_ptrs_stage_row" s
            WHERE s."customerId" = :customerId
              AND s."ptrsId" = :ptrsId
              AND s."profileId" = :profileId
              AND s."deletedAt" IS NULL
          ),
          term_lookup AS (
            SELECT
              ts."id",
              ts."termCode" AS "paymentTermRaw",
              CASE
                WHEN ts."termCode" ~ '^\\d{1,4}$' THEN ts."termCode"::int
                ELSE ptm."transformedDays"
              END AS "paymentTermDays"
            FROM term_source ts
            LEFT JOIN "tbl_ptrs_payment_term_map" ptm
              ON ptm."customerId" = :customerId
             AND ptm."profileId" = :profileId
             AND ptm."deletedAt" IS NULL
             AND ptm."raw" = ts."termCode"
          )
          UPDATE "tbl_ptrs_stage_row" s
          SET
            "paymentTermRaw" = tl."paymentTermRaw",
            "paymentTermDays" = tl."paymentTermDays",
            "updatedBy" = :userId,
            "updatedAt" = NOW()
          FROM term_lookup tl
          WHERE s."id" = tl."id"
            AND s."profileId" = :profileId
            AND s."deletedAt" IS NULL
        `;

      await db.sequelize.query(paymentTermSql, {
        transaction: t,
        replacements: {
          customerId,
          ptrsId,
          profileId,
          userId: userId || null,
        },
      });
      stageEnd(sPaymentTermUpdate, { updatedRows: persistedCount });

      const sPaymentTimeUpdate = stageStart(
        "persist_stage_update_payment_time",
      );
      const sqlDateExpr = (jsonKey) => `
          CASE
            WHEN NULLIF(s."data"->>'${jsonKey}', '') IS NULL THEN NULL
            WHEN (s."data"->>'${jsonKey}') ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (s."data"->>'${jsonKey}')::date
            WHEN (s."data"->>'${jsonKey}') ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN to_date(s."data"->>'${jsonKey}', 'DD/MM/YYYY')
            ELSE (
              SELECT pg_catalog.to_date('__INVALID__', 'YYYY-MM-DD')
              FROM pg_catalog.pg_class
              WHERE NULLIF(s."data"->>'${jsonKey}', '') IS NOT NULL
            )
          END
        `;

      const paymentDateExpr = sqlDateExpr("payment_date");
      const invoiceIssueDateExpr = sqlDateExpr("invoice_issue_date");
      const invoiceReceiptDateExpr = sqlDateExpr("invoice_receipt_date");
      const noticeForPaymentIssueDateExpr = sqlDateExpr(
        "notice_for_payment_issue_date",
      );
      const supplyDateExpr = sqlDateExpr("supply_date");
      const invoiceDueDateExpr = sqlDateExpr("invoice_due_date");
      const paymentTimeSql = `
          WITH payment_time AS (
            SELECT
              s."id",
              CASE
                WHEN ${paymentDateExpr} IS NULL THEN NULL
                WHEN LOWER(TRIM(COALESCE(s."data"->>'rcti', ''))) IN ('yes','y','true') THEN
                  CASE
                    WHEN ${invoiceIssueDateExpr} IS NULL THEN NULL
                    WHEN (${paymentDateExpr} - ${invoiceIssueDateExpr}) <= 0 THEN 0
                    ELSE (${paymentDateExpr} - ${invoiceIssueDateExpr}) + 1
                  END
                WHEN ${invoiceIssueDateExpr} IS NULL
                  AND ${noticeForPaymentIssueDateExpr} IS NULL THEN
                  CASE
                    WHEN COALESCE(
                      ${supplyDateExpr},
                      ${invoiceDueDateExpr}
                    ) IS NULL THEN NULL
                    WHEN (
                      ${paymentDateExpr}
                      - COALESCE(
                          ${supplyDateExpr},
                          ${invoiceDueDateExpr}
                        )
                    ) <= 0 THEN 0
                    ELSE (
                      ${paymentDateExpr}
                      - COALESCE(
                          ${supplyDateExpr},
                          ${invoiceDueDateExpr}
                        )
                    ) + 1
                  END
                WHEN ${invoiceIssueDateExpr} IS NULL THEN
                  CASE
                    WHEN ${noticeForPaymentIssueDateExpr} IS NULL THEN NULL
                    WHEN (
                      ${paymentDateExpr}
                      - ${noticeForPaymentIssueDateExpr}
                    ) <= 0 THEN 0
                    ELSE (
                      ${paymentDateExpr}
                      - ${noticeForPaymentIssueDateExpr}
                    ) + 1
                  END
                ELSE
                  CASE
                    WHEN ${invoiceReceiptDateExpr} IS NULL THEN
                      CASE
                        WHEN (
                          ${paymentDateExpr}
                          - ${invoiceIssueDateExpr}
                        ) <= 0 THEN 0
                        ELSE (
                          ${paymentDateExpr}
                          - ${invoiceIssueDateExpr}
                        ) + 1
                      END
                    ELSE
                      CASE
                        WHEN LEAST(
                          ${paymentDateExpr} - ${invoiceIssueDateExpr},
                          ${paymentDateExpr} - ${invoiceReceiptDateExpr}
                        ) <= 0 THEN 0
                        ELSE LEAST(
                          ${paymentDateExpr} - ${invoiceIssueDateExpr},
                          ${paymentDateExpr} - ${invoiceReceiptDateExpr}
                        ) + 1
                      END
                  END
              END AS "paymentTimeDays",
              CASE
                WHEN ${paymentDateExpr} IS NULL THEN NULL
                WHEN LOWER(TRIM(COALESCE(s."data"->>'rcti', ''))) IN ('yes','y','true') THEN ${invoiceIssueDateExpr}
                WHEN ${invoiceIssueDateExpr} IS NULL
                  AND ${noticeForPaymentIssueDateExpr} IS NULL THEN COALESCE(
                    ${supplyDateExpr},
                    ${invoiceDueDateExpr}
                  )
                WHEN ${invoiceIssueDateExpr} IS NULL THEN ${noticeForPaymentIssueDateExpr}
                WHEN ${invoiceReceiptDateExpr} IS NULL THEN ${invoiceIssueDateExpr}
                WHEN (
                  ${paymentDateExpr} - ${invoiceIssueDateExpr}
                ) <= (
                  ${paymentDateExpr} - ${invoiceReceiptDateExpr}
                ) THEN ${invoiceIssueDateExpr}
                ELSE ${invoiceReceiptDateExpr}
              END AS "referenceDate",
              CASE
                WHEN ${paymentDateExpr} IS NULL THEN NULL
                WHEN LOWER(TRIM(COALESCE(s."data"->>'rcti', ''))) IN ('yes','y','true') THEN 'invoice_issue'
                WHEN ${invoiceIssueDateExpr} IS NULL
                  AND ${noticeForPaymentIssueDateExpr} IS NULL
                  AND ${supplyDateExpr} IS NOT NULL THEN 'supply'
                WHEN ${invoiceIssueDateExpr} IS NULL
                  AND ${noticeForPaymentIssueDateExpr} IS NULL
                  AND ${invoiceDueDateExpr} IS NOT NULL THEN 'invoice_due'
                WHEN ${invoiceIssueDateExpr} IS NULL THEN 'notice_for_payment'
                WHEN ${invoiceReceiptDateExpr} IS NULL THEN 'invoice_issue'
                WHEN (
                  ${paymentDateExpr} - ${invoiceIssueDateExpr}
                ) <= (
                  ${paymentDateExpr} - ${invoiceReceiptDateExpr}
                ) THEN 'invoice_issue'
                ELSE 'invoice_receipt'
              END AS "referenceKind"
            FROM "tbl_ptrs_stage_row" s
            WHERE s."customerId" = :customerId
              AND s."ptrsId" = :ptrsId
              AND s."profileId" = :profileId
              AND s."deletedAt" IS NULL
          )
          UPDATE "tbl_ptrs_stage_row" s
          SET
            "paymentTimeDays" = pt."paymentTimeDays",
            "updatedBy" = :userId,
            "updatedAt" = NOW()
          FROM payment_time pt
          WHERE s."id" = pt."id"
            AND s."profileId" = :profileId
            AND s."deletedAt" IS NULL
        `;

      await db.sequelize.query(paymentTimeSql, {
        transaction: t,
        replacements: {
          customerId,
          ptrsId,
          profileId,
          userId: userId || null,
        },
      });
      stageEnd(sPaymentTimeUpdate, { updatedRows: persistedCount });
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
