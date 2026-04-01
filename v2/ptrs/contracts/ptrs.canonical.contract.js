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
      notes: "Transaction payment amount. Must be numeric and non-negative.",
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
  // E) Regulator classification flags (populated during report prep)
  // ---------------------------------------------------------------------
  regulator_flags: {
    trade_credit_payment: {
      type: "bool",
      required: false,
      notes:
        "Determined during report preparation. Indicates whether record qualifies as trade credit.",
    },

    excluded_trade_credit_payment: {
      type: "bool",
      required: false,
      notes: "True when the record is excluded from TCP calculations.",
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
  // F) Validation rules applied during canonical validation
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
