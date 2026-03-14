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
  db,
}) {
  let executionRun = null;
  let inputHash = null;

  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  if (persist && !profileId) {
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
        mappedRowUpdatedAt:
          staleness?.snapshot?.mappedRows?.maxUpdatedAt || null,
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
    }

    let stagedRows = [];
    let rulesStats = null;
    let paymentTermChangeStats = null;
    let paymentTermStats = null;
    let mapRow = null;

    if (!persist) {
      const sLoadMapped = stageStart("load_mapped_rows");
      const { rows: baseRows } = await loadMappedRowsForPtrs({
        customerId,
        ptrsId,
        limit: null,
        transaction: t,
      });
      stageEnd(sLoadMapped, {
        rowsCount: Array.isArray(baseRows) ? baseRows.length : 0,
      });

      const rows = baseRows;

      slog.info("PTRS v2 stagePtrs: loaded mapped rows", {
        action: "PtrsV2StagePtrsLoadedMappedRows",
        customerId,
        ptrsId,
        rowsCount: Array.isArray(rows) ? rows.length : 0,
        sampleRowKeys: rows && rows[0] ? Object.keys(rows[0]) : null,
      });

      const sLoadMap = stageStart("load_column_map");
      try {
        mapRow = await getColumnMap({ customerId, ptrsId, transaction: t });
      } catch (_) {
        mapRow = null;
      }
      stageEnd(sLoadMap, {
        hasMap: !!mapRow,
        hasRowRules: !!(mapRow && mapRow.rowRules),
        hasJoins: !!(mapRow && mapRow.joins),
      });

      stagedRows = rows;
    } else {
      try {
        mapRow = null;
      } catch (_) {
        mapRow = null;
      }
    }

    if (!persist) {
      try {
        let rowRules =
          mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
        if (typeof rowRules === "string") {
          try {
            rowRules = JSON.parse(rowRules);
          } catch {
            rowRules = null;
          }
        }

        const sRules = stageStart("apply_row_rules");
        const rulesResult = applyRules(
          stagedRows,
          Array.isArray(rowRules) ? rowRules : [],
        );
        stagedRows = rulesResult.rows || stagedRows;
        rulesStats = rulesResult.stats || null;
        stageEnd(sRules, {
          rowsOut: Array.isArray(stagedRows) ? stagedRows.length : 0,
          rulesCount: Array.isArray(rowRules) ? rowRules.length : 0,
          rulesStats: rulesResult.stats || null,
        });
      } catch (err) {
        slog.warn("PTRS v2 stagePtrs: failed to apply row rules", {
          action: "PtrsV2StagePtrsApplyRules",
          customerId,
          ptrsId,
          error: err.message,
        });
      }

      try {
        if (profileId) {
          const sTermChanges = stageStart("apply_payment_term_changes");
          const changeMap = await loadEffectiveTermChangesForRows({
            customerId,
            profileId,
            rows: stagedRows,
            mapRow,
            transaction: t,
          });

          const changeResult = applyEffectiveTermChangesToRows(
            stagedRows,
            changeMap,
            mapRow,
          );
          stagedRows = changeResult.rows || stagedRows;
          paymentTermChangeStats = changeResult.stats || null;

          stageEnd(sTermChanges, {
            applied: Number(changeResult?.stats?.applied) || 0,
            considered: Number(changeResult?.stats?.considered) || 0,
            missingKey: Number(changeResult?.stats?.missingKey) || 0,
          });

          if (paymentTermChangeStats?.applied) {
            slog.info(
              "PTRS v2 stagePtrs: applied effective-dated payment term changes",
              {
                action: "PtrsV2StagePtrsPaymentTermChangesApplied",
                customerId,
                ptrsId,
                profileId,
                ...paymentTermChangeStats,
                joinSpec: paymentTermChangeStats?.joinSpec || null,
              },
            );
          }
          if (!paymentTermChangeStats?.applied) {
            slog.info(
              "PTRS v2 stagePtrs: no effective-dated payment term changes applied",
              {
                action: "PtrsV2StagePtrsPaymentTermChangesNoneApplied",
                customerId,
                ptrsId,
                profileId,
                ...paymentTermChangeStats,
                joinSpec: paymentTermChangeStats?.joinSpec || null,
                note: "No matches found using company_code join key (and optional supplier) against tbl_ptrs_payment_term_change",
              },
            );
          }
        }
      } catch (err) {
        trace?.write("stage_payment_term_changes_error", {
          message: err?.message || null,
          statusCode: err?.statusCode || null,
        });
        slog.warn("PTRS v2 stagePtrs: failed to apply payment term changes", {
          action: "PtrsV2StagePtrsPaymentTermChangesFailed",
          customerId,
          ptrsId,
          profileId: profileId || null,
          error: err?.message,
        });
      }

      try {
        if (profileId) {
          const sTermMap = stageStart("apply_payment_term_map");
          const termMap = await loadPaymentTermMap({
            customerId,
            profileId,
            transaction: t,
          });

          const termResult = applyPaymentTermDaysFromMap(stagedRows, termMap);
          stagedRows = termResult.rows;
          paymentTermStats = termResult.stats;
          stageEnd(sTermMap, {
            lookedUp: Number(termResult?.stats?.lookedUp) || 0,
            filled: Number(termResult?.stats?.filled) || 0,
            missing: Number(termResult?.stats?.missing) || 0,
            unmapped: Number(termResult?.stats?.unmapped) || 0,
          });

          if (
            paymentTermStats?.missing ||
            paymentTermStats?.filled ||
            paymentTermStats?.unmapped
          ) {
            slog.info("PTRS v2 stagePtrs: payment term mapping stats", {
              action: "PtrsV2StagePtrsPaymentTermMap",
              customerId,
              ptrsId,
              profileId,
              ...paymentTermStats,
            });
          }
        } else {
          slog.warn(
            "PTRS v2 stagePtrs: profileId not provided; skipping payment term mapping",
            {
              action: "PtrsV2StagePtrsPaymentTermMapSkipped",
              customerId,
              ptrsId,
            },
          );
        }
      } catch (err) {
        trace?.write("stage_payment_term_map_error", {
          message: err?.message || null,
          statusCode: err?.statusCode || null,
        });
        slog.warn("PTRS v2 stagePtrs: failed to apply payment term mapping", {
          action: "PtrsV2StagePtrsPaymentTermMapFailed",
          customerId,
          ptrsId,
          profileId: profileId || null,
          error: err?.message,
        });
      }

      try {
        const sPaymentTime = stageStart("derive_payment_time_days");
        let derivedCount = 0;
        let underivedCount = 0;
        for (const r of stagedRows) {
          if (!r || typeof r !== "object") continue;

          const res = computePaymentTimeRegulator(r);
          if (res?.days == null) {
            if (r.payment_time_days == null) {
              if (!Array.isArray(r._stageErrors)) r._stageErrors = [];
              r._stageErrors.push({
                code: "PAYMENT_TIME_UNDERIVED",
                message:
                  "Payment time could not be derived using regulator rules (missing required date fields)",
                field: "payment_time_days",
                value: null,
              });
            }
            underivedCount += 1;
            continue;
          }

          derivedCount += 1;
          r.payment_time_days = res.days;
          if (res.referenceDate)
            r.payment_time_reference_date = res.referenceDate;
          if (res.referenceKind)
            r.payment_time_reference_kind = res.referenceKind;
        }
        stageEnd(sPaymentTime, {
          derivedCount,
          underivedCount,
          rows: Array.isArray(stagedRows) ? stagedRows.length : 0,
        });
      } catch (err) {
        trace?.write("stage_payment_time_error", {
          message: err?.message || null,
          statusCode: err?.statusCode || null,
        });
        slog.warn("PTRS v2 stagePtrs: failed to derive payment_time_days", {
          action: "PtrsV2StagePtrsPaymentTimeDerivationFailed",
          customerId,
          ptrsId,
          error: err?.message,
        });
      }
    }

    const contractStageFields = collectCanonicalContractFields(
      PTRS_CANONICAL_CONTRACT,
    );

    const derivedStageFields = [
      "payment_term_days",
      "payment_time_days",
      "payment_time_reference_date",
      "payment_time_reference_kind",
      "contract_po_payment_terms_effective",
      "contract_po_payment_terms_effective_source",
      "contract_po_payment_terms_effective_changed_at",
    ];

    let mappedExtraStageFields = [];
    try {
      if (profileId && db.PtrsFieldMap) {
        const fieldMapRows = await db.PtrsFieldMap.findAll({
          where: { customerId, ptrsId, profileId },
          attributes: ["canonicalField"],
          raw: true,
          transaction: t,
        });

        mappedExtraStageFields = Array.from(
          new Set(
            (fieldMapRows || [])
              .map((r) => toSnakeCase(r?.canonicalField))
              .filter(Boolean),
          ),
        );
      }
    } catch (err) {
      slog.warn(
        "PTRS v2 stagePtrs: failed to load canonical field-map rows for persisted stage shape",
        {
          action: "PtrsV2StagePtrsPersistShapeFieldMapLoadFailed",
          customerId,
          ptrsId,
          profileId: profileId || null,
          error: err?.message,
        },
      );
    }

    const persistedStageFields = Array.from(
      new Set([
        ...contractStageFields,
        ...mappedExtraStageFields,
        ...derivedStageFields,
      ]),
    );
    void persistedStageFields;

    let persistedCount = null;

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

    if (persist) {
      const defaultDatasetId = await resolveDefaultDatasetId();

      const missingDatasetIdCount = (stagedRows || []).reduce((acc, r) => {
        const did = r?.datasetId || r?.dataset_id || null;
        return acc + (did ? 0 : 1);
      }, 0);

      if (!defaultDatasetId) {
        const e = new Error("Unable to resolve datasetId for staging");
        e.statusCode = 500;
        throw e;
      }

      const sPersistClear = stageStart("persist_stage_clear");
      const clearedCount = await db.PtrsStageRow.destroy({
        where: { customerId, ptrsId },
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
          :defaultDatasetId AS "datasetId",
          m."rowNo" AS "rowNo",
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
        missingDatasetIdCount,
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
        persistedCount = Number(insertMeta?.rowCount || 0);
        stageEnd(sPersistInsert, { inserted: persistedCount });

        const sPaymentTimeUpdate = stageStart(
          "persist_stage_update_payment_time",
        );
        const sqlDateExpr = (jsonKey) => `
          CASE
            WHEN NULLIF(s."data"->>'${jsonKey}', '') IS NULL THEN NULL
            WHEN (s."data"->>'${jsonKey}') ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN (s."data"->>'${jsonKey}')::date
            WHEN (s."data"->>'${jsonKey}') ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_date(s."data"->>'${jsonKey}', 'DD/MM/YYYY')
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
              AND s."deletedAt" IS NULL
          )
          UPDATE "tbl_ptrs_stage_row" s
          SET
            "data" = jsonb_strip_nulls(
              COALESCE(s."data", '{}'::jsonb)
              || jsonb_build_object(
                'payment_time_days', pt."paymentTimeDays",
                'payment_time_reference_date', CASE WHEN pt."referenceDate" IS NOT NULL THEN pt."referenceDate"::text ELSE NULL END,
                'payment_time_reference_kind', pt."referenceKind"
              )
            ),
            "updatedBy" = :userId,
            "updatedAt" = NOW()
          FROM payment_time pt
          WHERE s."id" = pt."id"
            AND s."deletedAt" IS NULL
        `;

        await db.sequelize.query(paymentTimeSql, {
          transaction: t,
          replacements: {
            customerId,
            ptrsId,
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
    }

    const tookMs = Date.now() - started;
    if (!persist) {
      persistedCount = null;
    }

    if (executionRun?.id) {
      try {
        const sRunUpdate = stageStart("execution_run_update_success");
        const effectiveRows = persist
          ? Number(persistedCount || 0)
          : stagedRows.length;

        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "success",
          finishedAt: new Date(),
          rowsIn: effectiveRows,
          rowsOut: effectiveRows,
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

    const effectiveRows = persist
      ? Number(persistedCount || 0)
      : stagedRows.length;

    trace?.write("stage_before_commit", {
      rowsIn: effectiveRows,
      rowsOut: effectiveRows,
      persistedCount,
      totalMs: hrMsSince(jobStartNs),
      tookMs: Date.now() - started,
    });

    await t.commit();
    trace?.write("stage_committed", { totalMs: hrMsSince(jobStartNs) });
    if (trace) await trace.close();

    return {
      rowsIn: persist ? Number(persistedCount || 0) : stagedRows.length,
      rowsOut: persist ? Number(persistedCount || 0) : stagedRows.length,
      persistedCount,
      tookMs,
      sample: persist ? null : stagedRows[0] || null,
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
