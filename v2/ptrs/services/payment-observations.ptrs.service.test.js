jest.mock("@/db/database", () => ({
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));

const db = require("@/db/database");
const {
  PAYMENT_OBSERVATION_WORK_MEM,
  buildPaymentObservationSummaryCte,
  buildPaymentObservationsCte,
  getPaymentObservationSummary,
  listPaymentObservationLinks,
  listPaymentObservations,
} = require("./payment-observations.ptrs.service");

describe("PTRS derived payment observations", () => {
  test("matches RE invoices to one ZP using the authoritative clearing key", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain("payment_observation_settlement_keys AS MATERIALIZED");
    expect(sql).toContain("settlement.settlement_row_count = 1");
    expect(sql).toContain(
      "document_type = :paymentObservationSettlementType",
    );
    expect(sql).toContain("invoice.document_type = :paymentObservationInvoiceType");
  });

  test("derives one observation per RE while retaining the shared ZP trace", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain(
      "'payment-observation:' || invoice.\"id\" AS \"observationId\"",
    );
    expect(sql).toContain(
      "invoice.\"id\" AS \"sourceInvoiceStageRowId\"",
    );
    expect(sql).toContain(
      "ARRAY[invoice.settlement_stage_row_id] AS \"settlementStageRowIds\"",
    );
    expect(sql).toContain(
      "ARRAY[invoice.\"id\", invoice.settlement_stage_row_id]",
    );
    expect(sql).not.toContain("DISTINCT ON (invoice");
  });

  test("exposes each eligible ZP settlement amount once per clearing group", () => {
    const sql = buildPaymentObservationsCte();
    const groupCte = sql.slice(
      sql.indexOf("payment_observation_accounting_settlement_groups AS"),
    );

    expect(sql).toContain('MAX("paymentAmount")');
    expect(groupCte).toContain("SELECT DISTINCT");
    expect(groupCte).toContain(
      'observation.company_code AS "sourceCompanyCode"',
    );
    expect(groupCte).toContain(
      'observation.source_account_code AS "sourceAccountCode"',
    );
    expect(groupCte).toContain(
      'observation.clearing_document AS "clearingDocument"',
    );
    expect(groupCte).toContain(
      'observation.settlement_payment_amount AS "settlementPaymentAmount"',
    );
    expect(groupCte).toContain(
      "FROM payment_observation_accounting_keys observation",
    );
  });

  test("takes payment date from ZP and excludes the established four-field ET match", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain("to_jsonb(invoice.settlement_payment_date)");
    expect(sql).toContain(
      "earlytrade.description_reference = invoice.description_reference",
    );
    expect(sql).toContain("payment_observation_earlytrade_matches AS MATERIALIZED");
    expect(sql).toContain("LEFT JOIN payment_observation_earlytrade_keys earlytrade");
    expect(sql).toContain('AND earlytrade."id" IS NULL');
  });

  test("isolates identical accounting keys by explicit scope or dataset fallback", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain("NULLIF(BTRIM(s.\"sourceGroupScope\"), '')");
    expect(sql).toContain("'dataset:' || s.\"datasetId\"");
    expect(sql).toContain(
      "settlement.source_group_key = invoice.source_group_key",
    );
  });

  test("projects every surviving direct-payment Stage row one-to-one without SAP event fabrication", () => {
    const sql = buildPaymentObservationsCte();
    const directKeysCte = sql.slice(
      sql.indexOf("payment_observation_direct_keys AS"),
      sql.indexOf("payment_observation_accounting_observations AS"),
    );
    const directCte = sql.slice(
      sql.indexOf("payment_observation_direct_observations AS"),
      sql.indexOf("payment_observations AS"),
    );

    expect(directKeysCte).toContain(
      "WHERE direct.\"semanticKind\" = 'direct_payment'",
    );
    expect(directKeysCte).toContain("AND NOT direct.excluded");
    expect(directCte).toContain(
      "'payment-observation:' || direct.\"id\" AS \"observationId\"",
    );
    expect(directCte).toContain(
      "direct.\"id\" AS \"primarySourceStageRowId\"",
    );
    expect(directCte).toContain(
      "NULL::varchar AS \"sourceInvoiceStageRowId\"",
    );
    expect(directCte).toContain(
      "ARRAY[]::varchar[] AS \"settlementStageRowIds\"",
    );
    expect(directCte).not.toContain(":paymentObservationInvoiceType");
    expect(directCte).not.toContain(":paymentObservationSettlementType");
  });

  test("retains complete canonical, raw-row, adapter, scope and joined-reference provenance", () => {
    const sql = buildPaymentObservationsCte();

    for (const field of [
      "canonicalRevisionId",
      "canonicalSourceRowId",
      "datasetId",
      "sourceRawRowId",
      "sourceRowNo",
      "adapterType",
      "adapterVersion",
      "semanticKind",
      "sourceGroupScope",
      "joinedReferences",
    ]) {
      expect(sql).toContain(`'${field}'`);
    }
    expect(sql).toContain("jsonb_build_array(\n          jsonb_build_object(");
    expect(sql).toContain(
      "'stageRowId', settlement_payload.\"id\"",
    );
  });

  test("keeps matching intermediates narrow and joins full payload only after eligibility", () => {
    const sql = buildPaymentObservationsCte();
    const sourceCte = sql.slice(
      sql.indexOf("payment_observation_source_rows AS"),
      sql.indexOf("payment_observation_settlement_keys AS"),
    );

    expect(sql).toContain("payment_observation_source_rows AS NOT MATERIALIZED");
    expect(sourceCte).not.toContain('s."data", s."meta"');
    expect(sourceCte).toContain('s."documentType"');
    expect(sourceCte).toContain('s."sourceAccountCode"');
    expect(sourceCte).toContain('s."clearingDocument"');
    expect(sql.indexOf('JOIN "tbl_ptrs_stage_row" invoice_payload')).toBeGreaterThan(
      sql.indexOf("payment_observation_accounting_keys AS"),
    );
    expect(sql).not.toContain("jsonb_agg");
    expect(sql).not.toContain("ARRAY_AGG");
    expect(sql).not.toContain("payment_observation_settlement_rows");
    expect(sql).not.toContain("payment_observation_settlement_anchors");
    expect(sql).not.toContain("OVER settlement_group");
    expect(sql).not.toContain("OVER earlytrade_group");
  });

  test("builds all summary counts in one aggregate without eligibility joins", async () => {
    const summaryCte = buildPaymentObservationSummaryCte();
    expect(summaryCte).toContain("payment_observation_summary_counts AS");
    expect(summaryCte).toContain("COUNT(*) FILTER");
    expect(summaryCte).toContain("OVER settlement_group");
    expect(summaryCte).toContain("OVER earlytrade_group");
    expect(summaryCte).not.toContain(" JOIN ");
    expect(summaryCte).not.toContain("SELECT DISTINCT");
    expect(summaryCte).not.toContain("GROUP BY");
    expect(summaryCte).not.toContain("UNION ALL");
    expect(summaryCte).not.toContain("jsonb_build_object");
    expect(summaryCte).not.toContain("jsonb_agg");
    expect(summaryCte).not.toContain("ARRAY_AGG");
    expect(summaryCte).not.toContain('s."paymentDate"::text');

    const summary = {
      sourceStageRows: 309280,
      derivedPaymentObservations: 1063,
    };
    db.sequelize.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([summary]);
    const transaction = { id: "tx-1" };
    await expect(
      getPaymentObservationSummary({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        transaction,
      }),
    ).resolves.toBe(summary);
    expect(db.sequelize.query.mock.calls[0]).toEqual([
      `SET LOCAL work_mem = '${PAYMENT_OBSERVATION_WORK_MEM}'`,
      { transaction },
    ]);
    const sql = db.sequelize.query.mock.calls[1][0];
    expect(sql).toMatch(/WITH\s+payment_observation_summary_source AS/);
    expect(sql).toContain("SELECT * FROM payment_observation_summary_counts");
    expect(sql).not.toContain("(SELECT COUNT(*)");
    expect(sql).not.toContain(
      "payment_observation_accounting_observations",
    );
  });

  test("uses the authoritative settlement and description-specific ET keys", () => {
    const sql = buildPaymentObservationSummaryCte();

    expect(sql).toContain(
      "source_group_key, company_code, source_account_code,\n            clearing_document",
    );
    expect(sql).toContain(
      "clearing_document, description_reference",
    );
    expect(sql).toContain("settlement_row_count = 1");
    expect(sql).toContain("AND NOT has_matching_earlytrade");
    expect(sql).toContain("AND has_matching_earlytrade");
  });

  test("unions accounting and direct observations into one format-neutral population", () => {
    const sql = buildPaymentObservationsCte();
    const combinedCte = sql.slice(
      sql.indexOf("payment_observations AS"),
      sql.indexOf("payment_observation_accounting_settlement_groups AS"),
    );

    expect(combinedCte).toContain(
      "SELECT * FROM payment_observation_accounting_observations",
    );
    expect(combinedCte).toContain("UNION ALL");
    expect(combinedCte).toContain(
      "SELECT * FROM payment_observation_direct_observations",
    );
  });

  test("uses one SAP settlement group and one actual amount per direct payment in the TCP denominator", () => {
    const sql = buildPaymentObservationsCte();
    const settlementCte = sql.slice(
      sql.indexOf("payment_observation_accounting_settlement_groups AS"),
    );

    expect(settlementCte).toContain("SELECT DISTINCT");
    expect(settlementCte).toContain(
      'observation.settlement_payment_amount AS "settlementPaymentAmount"',
    );
    expect(settlementCte).toContain("UNION ALL");
    expect(settlementCte).toContain(
      "'payment-observation:' || direct.\"id\" AS \"settlementIdentity\"",
    );
    expect(settlementCte).toContain(
      'direct."paymentAmount" AS "settlementPaymentAmount"',
    );
    expect(sql).toContain(
      "'paymentAmountSemantic', 'actual_settlement_amount'",
    );
  });

  test("executes as one tenant-scoped set-based query", async () => {
    const rows = [{ observationId: "payment-observation:re-1" }];
    db.sequelize.query.mockResolvedValueOnce([]).mockResolvedValueOnce(rows);

    const result = await listPaymentObservations({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      transaction: { id: "tx-1" },
    });

    expect(result).toBe(rows);
    expect(db.sequelize.query).toHaveBeenCalledTimes(2);
    expect(db.sequelize.query.mock.calls[0]).toEqual([
      `SET LOCAL work_mem = '${PAYMENT_OBSERVATION_WORK_MEM}'`,
      { transaction: { id: "tx-1" } },
    ]);
    expect(db.sequelize.query.mock.calls[1][1]).toMatchObject({
      transaction: { id: "tx-1" },
      replacements: {
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        paymentObservationInvoiceType: "RE",
        paymentObservationSettlementType: "ZP",
        paymentObservationEarlytradeType: "ET",
      },
      type: "SELECT",
    });
  });

  test("lists narrow transformation-history links without materialising payload", async () => {
    const links = [{ observationId: "payment-observation:re-1" }];
    db.sequelize.query.mockResolvedValueOnce([]).mockResolvedValueOnce(links);

    await expect(
      listPaymentObservationLinks({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        transaction: { id: "tx-2" },
      }),
    ).resolves.toBe(links);

    const sql = db.sequelize.query.mock.calls[1][0];
    const select = sql.slice(
      sql.lastIndexOf(
        "SELECT\n      'payment-observation:' || invoice.\"id\"",
      ),
    );
    expect(select).toContain("FROM payment_observation_accounting_keys invoice");
    expect(select).toContain("FROM payment_observation_direct_keys direct");
    expect(select).not.toContain("sourceProvenance");
    expect(select).not.toContain('invoice_payload."data"');
    expect(select).not.toContain('direct_payload."meta"');
  });
});
