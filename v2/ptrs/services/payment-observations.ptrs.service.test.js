jest.mock("@/db/database", () => ({
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));

const db = require("@/db/database");
const {
  buildPaymentObservationsCte,
  listPaymentObservations,
} = require("./payment-observations.ptrs.service");

describe("PTRS derived payment observations", () => {
  test("matches RE invoices to one ZP using the authoritative clearing key", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain(
      "settlement.company_code = invoice.company_code",
    );
    expect(sql).toContain(
      "settlement.source_group_key = invoice.source_group_key",
    );
    expect(sql).toContain(
      "settlement.source_account_code = invoice.source_account_code",
    );
    expect(sql).toContain(
      "settlement.clearing_document = invoice.clearing_document",
    );
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
      "settlement.settlement_stage_row_ids AS \"settlementStageRowIds\"",
    );
    expect(sql).toContain(
      "ARRAY[invoice.\"id\"] || settlement.settlement_stage_row_ids",
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
      'settlement.company_code AS "sourceCompanyCode"',
    );
    expect(groupCte).toContain(
      'settlement.source_account_code AS "sourceAccountCode"',
    );
    expect(groupCte).toContain(
      'settlement.clearing_document AS "clearingDocument"',
    );
    expect(groupCte).toContain(
      'settlement.settlement_payment_amount AS "settlementPaymentAmount"',
    );
    expect(groupCte).toContain(
      "JOIN payment_observation_accounting_observations observation",
    );
  });

  test("takes payment date from ZP and excludes the established four-field ET match", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain("to_jsonb(settlement.payment_date)");
    expect(sql).toContain("earlytrade.company_code = invoice.company_code");
    expect(sql).toContain(
      "earlytrade.source_account_code = invoice.source_account_code",
    );
    expect(sql).toContain(
      "earlytrade.clearing_document = invoice.clearing_document",
    );
    expect(sql).toContain(
      "earlytrade.description_reference = invoice.description_reference",
    );
    expect(sql).toContain("earlytrade.company_code IS NULL");
    expect(sql).toContain(
      "earlytrade.source_group_key = invoice.source_group_key",
    );
  });

  test("isolates identical accounting keys by explicit scope or dataset fallback", () => {
    const sql = buildPaymentObservationsCte();

    expect(sql).toContain("NULLIF(BTRIM(s.\"sourceGroupScope\"), '')");
    expect(sql).toContain("'dataset:' || s.\"datasetId\"");
    expect(sql).toContain(
      "GROUP BY\n        source_group_key, company_code, source_account_code, clearing_document",
    );
    expect(sql).toContain(
      "settlement.source_group_key = invoice.source_group_key",
    );
  });

  test("projects every surviving direct-payment Stage row one-to-one without SAP event fabrication", () => {
    const sql = buildPaymentObservationsCte();
    const directCte = sql.slice(
      sql.indexOf("payment_observation_direct_observations AS"),
      sql.indexOf("payment_observations AS"),
    );

    expect(directCte).toContain(
      "WHERE direct.\"semanticKind\" = 'direct_payment'",
    );
    expect(directCte).toContain("AND NOT direct.excluded");
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
    expect(sql).toContain(
      "jsonb_build_array(direct.source_provenance) AS \"sourceProvenance\"",
    );
    expect(sql).toContain(
      "jsonb_build_array(invoice.source_provenance)\n          || settlement.settlement_source_provenance",
    );
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
      'settlement.settlement_payment_amount AS "settlementPaymentAmount"',
    );
    expect(settlementCte).toContain("UNION ALL");
    expect(settlementCte).toContain(
      'direct."observationId" AS "settlementIdentity"',
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
    db.sequelize.query.mockResolvedValue(rows);

    const result = await listPaymentObservations({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      transaction: { id: "tx-1" },
    });

    expect(result).toBe(rows);
    expect(db.sequelize.query).toHaveBeenCalledTimes(1);
    expect(db.sequelize.query.mock.calls[0][1]).toMatchObject({
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
});
