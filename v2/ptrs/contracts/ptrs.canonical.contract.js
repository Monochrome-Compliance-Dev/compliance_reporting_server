const PTRS_CANONICAL_CONTRACT = {
  version: "2.0",

  // ---------------------------------------------------------------------
  // A) Core identity (required for a valid TCP row)
  // ---------------------------------------------------------------------
  identity: {
    payer_entity_name: {
      type: "string",
      required: true,
      notes: "Reporting entity name (payer). Mandatory for TCP row validity.",
    },

    payer_entity_abn: {
      type: "string",
      required: true,
      notes: "Reporting entity ABN (payer). Mandatory for TCP row validity.",
    },

    payer_entity_acn_arbn: {
      type: "string",
      required: false,
      notes: "Optional ACN/ARBN for payer.",
    },

    payee_entity_name: {
      type: "string",
      required: true,
      notes: "Supplier name (payee). Mandatory for TCP row validity.",
    },

    payee_entity_abn: {
      type: "string",
      required: true,
      notes: "Supplier ABN (payee). Mandatory for TCP row validity.",
    },

    payee_entity_acn_arbn: {
      type: "string",
      required: false,
      notes: "Optional ACN/ARBN for payee.",
    },

    invoice_reference_number: {
      type: "string",
      required: true,
      notes:
        "Unique transaction identifier. Usually invoice number. For bank transactions (e.g. Xero), may be payment or transaction reference.",
    },
  },

  // ---------------------------------------------------------------------
  // B) Transaction values
  // ---------------------------------------------------------------------
  transaction: {
    payment_amount: {
      type: "money",
      required: true,
      notes:
        "Final interpreted transaction payment amount for the TCP row. Must be numeric and non-negative after normalisation. Source-system rows may contain signed amounts and should be normalised during staging or report preparation.",
    },

    description: {
      type: "string",
      required: false,
      notes:
        "Free text description. Not required but extremely useful for classification and investigation.",
    },
  },

  // ---------------------------------------------------------------------
  // C) Raw dates used to determine payment start-of-clock
  // ---------------------------------------------------------------------
  dates: {
    payment_date: {
      type: "date",
      required: true,
      notes: "Mandatory payment date.",
    },

    supply_date: {
      type: "date",
      required: false,
      notes:
        "Fallback reference date if invoice and notice dates are unavailable.",
    },

    notice_for_payment_issue_date: {
      type: "date",
      required: false,
      notes: "Fallback reference date if invoice dates are unavailable.",
    },

    invoice_issue_date: {
      type: "date",
      required: false,
      notes: "Primary invoice reference date candidate.",
    },

    invoice_receipt_date: {
      type: "date",
      required: false,
      notes:
        "Primary invoice reference date candidate (shorter-of rule with invoice_issue_date).",
    },

    invoice_due_date: {
      type: "date",
      required: false,
      notes: "Optional regulator dataset field.",
    },
  },

  // ---------------------------------------------------------------------
  // D) Raw payment terms sources
  // ---------------------------------------------------------------------
  terms: {
    contract_po_reference_number: {
      type: "string",
      required: false,
      notes: "Optional contract or purchase order reference.",
    },

    contract_po_payment_terms: {
      type: "string",
      required: false,
      notes: "Payment terms defined in contract or PO.",
    },

    notice_for_payment_terms: {
      type: "string",
      required: false,
      notes: "Payment terms stated in payment notice.",
    },

    invoice_payment_terms: {
      type: "string",
      required: false,
      notes: "Payment terms stated on invoice.",
    },

    payment_term: {
      type: "string",
      required: false,
      notes: "Generic payment term field often present in source datasets.",
    },
  },

  // ---------------------------------------------------------------------
  // E) Optional operational source fields (not canonical business truth,
  //    but highly useful for staging, exclusions, and investigation)
  // ---------------------------------------------------------------------
  operational_source_fields: {
    document_type: {
      type: "string",
      required: false,
      notes:
        "Optional source-system document classification (for example SAP document type such as RE, KR, KG, ZP). Not required for TCP validity, but highly useful for exclusion logic, transaction classification, and debugging.",
    },

    clearing_document: {
      type: "string",
      required: false,
      notes:
        "Optional source-system clearing document identifier. Useful for pairing invoice and settlement events, exclusion logic, and identifying internal clearing or non-payment artefacts.",
    },

    source_account_code: {
      type: "string",
      required: false,
      notes:
        "Optional source-system supplier or account code. Useful for pairing, source investigation, and identifying wrong-vendor or duplicate-account correction chains.",
    },

    invoice_created_date: {
      type: "date",
      required: false,
      notes:
        "Optional processing field. Source invoice-created date used during staging to resolve invoice_receipt_date for source systems such as Ariba where the invoice creation date is the preferred receipt-date proxy.",
    },

    entry_date: {
      type: "date",
      required: false,
      notes:
        "Optional processing field. Source entry date used during staging to resolve invoice_receipt_date where no more specific invoice-created date is available.",
    },

    reconciliation_status: {
      type: "string",
      required: false,
      notes:
        "Optional processing field. Source reconciliation status used during staging for source-system eligibility or exclusion logic, including Ariba Paying/Paid filtering.",
    },

    source_user: {
      type: "string",
      required: false,
      notes:
        "Optional processing field. Source-system user or integration identifier used for staging investigation and source-specific processing logic where required.",
    },
  },
  // ---------------------------------------------------------------------
  // F) Regulator classification flags (populated during report prep)
  // ---------------------------------------------------------------------
  regulator_flags: {
    trade_credit_payment: {
      type: "bool",
      required: false,
      notes:
        "Determined during report preparation. Indicates whether the record qualifies as trade credit after considering the transaction shape and any relevant source-system context such as document type, clearing behaviour, same-day settlement, and other accounting artefacts.",
    },

    excluded_trade_credit_payment: {
      type: "bool",
      required: false,
      notes:
        "True when the record is excluded from TCP calculations. Exclusion may depend on report-preparation rules such as non-payment accounting entries, reversals, same-day settlements, internal clearing artefacts, or other source-system behaviours that do not represent genuine payment performance.",
    },

    peppol_einvoice_enabled: {
      type: "bool",
      required: false,
      notes: "Optional PEPPOL capability indicator.",
    },

    rcti: {
      type: "bool",
      required: false,
      notes: "Recipient created tax invoice indicator.",
    },

    credit_card_payment: {
      type: "bool",
      required: false,
      notes: "Indicates whether the payment was made via credit card.",
    },

    credit_card_no: {
      type: "string",
      required: false,
      notes: "Optional credit card identifier field.",
    },

    partial_payment: {
      type: "bool",
      required: false,
      notes: "Indicates whether the payment represents a partial settlement.",
    },
  },

  // ---------------------------------------------------------------------
  // G) Validation rules applied during canonical validation
  // ---------------------------------------------------------------------
  rules: {
    required_identity_fields: [
      "payer_entity_name",
      "payer_entity_abn",
      "payee_entity_name",
      "payee_entity_abn",
      "invoice_reference_number",
    ],

    required_transaction_fields: ["payment_amount", "payment_date"],

    reference_date_sources: [
      "invoice_issue_date",
      "invoice_receipt_date",
      "notice_for_payment_issue_date",
      "supply_date",
    ],

    payment_term_sources: [
      "contract_po_payment_terms",
      "notice_for_payment_terms",
      "invoice_payment_terms",
      "payment_term",
    ],

    notes: {
      reference_date_rule:
        "Payment start-of-clock must be derivable from at least one reference date source. Preferred rule: shorter-of invoice issue/receipt where both exist; otherwise fallback to notice, then supply.",

      terms_rule:
        "At least one payment term source should exist unless supplier-level default terms are provided via supporting datasets.",
    },
  },
};

module.exports = {
  PTRS_CANONICAL_CONTRACT,
};
