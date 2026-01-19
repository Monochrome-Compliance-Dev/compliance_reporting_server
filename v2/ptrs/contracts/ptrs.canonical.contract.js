const PTRS_CANONICAL_CONTRACT = {
  version: "1.1",
  fields: {
    // ---------------------------------------------------------------------
    // A) Transaction identity (record validity / traceability)
    // ---------------------------------------------------------------------
    payer_entity_name: {
      type: "string",
      required_for_report: true,
      required_for_metrics: false,
      derived_allowed: true, // via supporting dataset joins if needed
      metrics_dependencies: [],
      notes: "Regulator dataset field. Required for record validity/reporting.",
    },

    payer_entity_abn: {
      type: "string", // ABN-format string; validate separately
      required_for_report: true,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Regulator dataset field. Required for record validity/reporting.",
    },

    payer_entity_acn_arbn: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    payee_entity_name: {
      type: "string",
      required_for_report: true,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Regulator dataset field. Required for record validity/reporting.",
    },

    payee_entity_abn: {
      type: "string",
      required_for_report: true,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [
        "percentageOfSmallBusinessTradeCreditPayments (via SBI + SB flagging)",
      ],
      notes:
        "Used operationally for SBI export/join; regulator dataset field for record validity.",
    },

    payee_entity_acn_arbn: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    invoice_reference_number: {
      type: "string",
      required_for_report: true,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Must exist by Validate/Report to identify each record deterministically (rules, audit, traceability). May come from a raw column, a custom/computed field, supporting dataset, or system-generated surrogate.",
    },

    // ---------------------------------------------------------------------
    // B) Amounts / description
    // ---------------------------------------------------------------------
    payment_amount: {
      type: "money",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: ["percentageOfSmallBusinessTradeCreditPayments"],
      notes:
        "Must be numeric and absolute (>=0). Canonical transform must normalise strings/currency formats before metrics.",
    },

    description: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Regulator dataset field; informational.",
    },

    // ---------------------------------------------------------------------
    // C) Dates (raw regulator fields + resolved reference date)
    // ---------------------------------------------------------------------
    payment_date: {
      type: "date",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: [
        "averagePaymentTimeDays",
        "medianPaymentTimeDays",
        "p80PaymentTimeDays",
        "p95PaymentTimeDays",
        "payments30DaysOrLessPct",
        "payments31To60DaysPct",
        "paymentsMoreThan60DaysPct",
        "percentageOfSbInvoicesPaidWithinPaymentTerm",
      ],
      notes: "Required. Used to compute payment_time_days (or validate it).",
    },

    supply_date: {
      type: "date",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional raw date. Used as last fallback to derive payment_time_reference_date if invoice/notice dates are missing.",
    },

    notice_for_payment_issue_date: {
      type: "date",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional raw date. Used as fallback to derive payment_time_reference_date if invoice dates missing.",
    },

    invoice_issue_date: {
      type: "date",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional raw date. Used (with invoice_receipt_date) to compute payment_time_reference_date per regulator rules.",
    },

    invoice_receipt_date: {
      type: "date",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional raw date. Used (with invoice_issue_date) to compute payment_time_reference_date per regulator rules (shorter-of).",
    },

    invoice_due_date: {
      type: "date",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    // Resolved “group” date for deterministic downstream computation
    payment_time_reference_date: {
      type: "date",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: [
        "averagePaymentTimeDays",
        "medianPaymentTimeDays",
        "p80PaymentTimeDays",
        "p95PaymentTimeDays",
        "payments30DaysOrLessPct",
        "payments31To60DaysPct",
        "paymentsMoreThan60DaysPct",
        "percentageOfSbInvoicesPaidWithinPaymentTerm",
      ],
      notes:
        "Canonical resolved start-of-clock date used to compute payment_time_days. Must be derived upstream (not in metrics). Rule: choose per regulator logic (shorter-of invoice issue/receipt where both exist; else notice; else supply).",
    },

    payment_time_reference_kind: {
      type: "enum",
      enum: [
        "invoice_issue",
        "invoice_receipt",
        "notice",
        "supply",
        "invoice_due",
      ],
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional provenance. Useful for Validate/QA and user trust. Not required for calculations.",
    },

    // ---------------------------------------------------------------------
    // D) Terms (raw text fields + resolved numeric days)
    // ---------------------------------------------------------------------
    contract_po_reference_number: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    contract_po_payment_terms: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional regulator dataset field. Do not use directly in metrics.",
    },

    notice_for_payment_terms: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional regulator dataset field. Do not use directly in metrics.",
    },

    invoice_payment_terms: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional regulator dataset field. Do not use directly in metrics.",
    },

    payment_term: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional. Often present in main dataset. Canonical should keep it, but metrics uses payment_term_days only.",
    },

    payment_term_days: {
      type: "int",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: [
        "commonPaymentTermsDays (mode)",
        "commonPaymentTermMinimum",
        "commonPaymentTermMaximum",
        "percentageOfSbInvoicesPaidWithinPaymentTerm",
      ],
      notes:
        "Must be resolved upstream. Can be parsed from payment_term/terms strings or derived via precedence rules. In MVP, if missing, metrics returns quality.blocked=true and null computed values.",
    },

    payment_term_source: {
      type: "enum",
      enum: [
        "explicit_days",
        "parsed_payment_term",
        "contract_po",
        "notice",
        "invoice_terms",
        "unknown",
      ],
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "Optional provenance; helps explain how payment_term_days was determined.",
    },

    // ---------------------------------------------------------------------
    // E) Regulator shaping flags (TCP / SBTCP)
    // ---------------------------------------------------------------------
    trade_credit_payment: {
      type: "bool",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: [
        "percentageOfSmallBusinessTradeCreditPayments (denominator shape)",
      ],
      notes:
        "Defines inclusion in trade credit totals. Must be canonical; no guessing from doc types in metrics.",
    },

    excluded_trade_credit_payment: {
      type: "bool",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: ["all computed metrics (exclusion filter)"],
      notes:
        "Regulator dataset field. True means excluded from totals/metrics. If you also support exclude_from_metrics, they should align.",
    },

    peppol_einvoice_enabled: {
      type: "bool",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [
        "percentagePeppolEnabledSmallBusinessProcurement (if implemented)",
      ],
      notes:
        "Optional in MVP. If missing, metric should be null with a quality note rather than guessed.",
    },

    rcti: {
      type: "bool",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    credit_card_payment: {
      type: "bool",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    credit_card_no: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    partial_payment: {
      type: "bool",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Optional regulator dataset field.",
    },

    // ---------------------------------------------------------------------
    // F) SBTCP additions (post-SBI + derived payment time)
    // ---------------------------------------------------------------------
    is_small_business: {
      type: "bool",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true, // via SBI import outcome
      metrics_dependencies: ["all SB-only metrics"],
      notes:
        "Post-SBI final classification. Must be boolean by Metrics step; unknown should be flagged as quality blocker.",
    },

    payment_time_days: {
      type: "int",
      required_for_report: true,
      required_for_metrics: true,
      derived_allowed: true,
      metrics_dependencies: [
        "averagePaymentTimeDays",
        "medianPaymentTimeDays",
        "p80PaymentTimeDays",
        "p95PaymentTimeDays",
        "payments30DaysOrLessPct",
        "payments31To60DaysPct",
        "paymentsMoreThan60DaysPct",
        "percentageOfSbInvoicesPaidWithinPaymentTerm",
      ],
      notes:
        "Derived canonically from payment_time_reference_date and payment_date using regulator logic (incl. shorter-of where invoice dates exist; clamp to 0 when negative). Metrics should not recompute this.",
    },

    // ---------------------------------------------------------------------
    // G) System controls (MVP-friendly, explicit)
    // ---------------------------------------------------------------------
    exclude_from_metrics: {
      type: "bool",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: ["all computed metrics (filter)"],
      notes:
        "System/user control flag. Prefer this long-term over relying on meta.rules.exclude in downstream steps.",
    },

    exclude_comment: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes:
        "System-populated at the time a record is marked to be excluded. Used for audit/debug and user trust.",
    },

    exclude_set_at: {
      type: "datetime",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Recommended for auditability when exclusion is applied.",
    },

    exclude_set_by: {
      type: "string",
      required_for_report: false,
      required_for_metrics: false,
      derived_allowed: true,
      metrics_dependencies: [],
      notes: "Recommended for auditability (user/system identifier).",
    },
  },

  // -----------------------------------------------------------------------
  // Row-level requiredness rules (applied during Validate / Metrics)
  // -----------------------------------------------------------------------
  rules: {
    // Defines which rows count for SB metrics (SBTCP-like)
    is_sb_trade_credit_row: [
      "trade_credit_payment === true",
      "excluded_trade_credit_payment !== true",
      "exclude_from_metrics !== true",
      "is_small_business === true",
    ],

    // For any row that is included in SB metrics, we must have:
    required_for_sb_metrics: [
      "payment_amount (money)",
      "payment_date (date)",
      "payment_time_reference_date (date)",
      "payment_time_days (int)",
      "payment_term_days (int)",
    ],

    // For any row that is included in trade credit totals (TCP-like):
    is_trade_credit_total_row: [
      "trade_credit_payment === true",
      "excluded_trade_credit_payment !== true",
      "exclude_from_metrics !== true",
    ],
  },
};

module.exports = {
  PTRS_CANONICAL_CONTRACT,
};
