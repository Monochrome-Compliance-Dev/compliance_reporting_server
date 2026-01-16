const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const xeroClient = require("@/v2/core/xero/xeroClient.service");
const xeroApi = require("@/v2/core/xero/xeroApi");
const {
  paginateXeroApi,
  callXeroApiWithAutoRefresh,
} = require("@/v2/core/xero/xeroApiUtils");

const statusStore = new Map();
const selectionStore = new Map();

function getXeroExtractLimit() {
  const raw = process.env.XERO_EXTRACT_LIMIT;

  if (raw === undefined || raw === null || raw === "") return null;

  const n = Number(raw);

  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("XERO_EXTRACT_LIMIT must be a positive number if set");
  }

  return Math.floor(n);
}

function applyExtractLimit(items, limit, state) {
  // `state` is a small mutable counter object e.g. { count: 0 }
  if (!limit) return { items, done: false };

  const remaining = Math.max(limit - (state?.count || 0), 0);
  const sliced = remaining > 0 ? items.slice(0, remaining) : [];

  if (state) state.count = (state.count || 0) + sliced.length;

  const done = state?.count >= limit;

  return { items: sliced, done };
}

function getSelectedTenantIds(customerId, ptrsId) {
  const key = statusKey(customerId, ptrsId);
  const selected = selectionStore.get(key);
  return Array.isArray(selected) ? selected.filter(Boolean) : [];
}

function setSelectedTenantIds(customerId, ptrsId, tenantIds) {
  const key = statusKey(customerId, ptrsId);
  selectionStore.set(
    key,
    Array.isArray(tenantIds) ? tenantIds.filter(Boolean) : []
  );
}

function updateStatus(customerId, ptrsId, patch) {
  const key = statusKey(customerId, ptrsId);
  const current = statusStore.get(key) || {
    ptrsId,
    status: "NOT_STARTED",
    message: null,
    progress: null,
    updatedAt: new Date().toISOString(),
  };

  const next = {
    ...current,
    ...patch,
    progress: {
      ...(current.progress || {}),
      ...(patch.progress || {}),
    },
    updatedAt: new Date().toISOString(),
  };

  statusStore.set(key, next);
  return next;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function base64(str) {
  return Buffer.from(String(str || ""), "utf8").toString("base64");
}

async function refreshAccessTokenIfNeeded({ customerId, tenantId, tokenRow }) {
  // tokenRow is the active (revoked=null) token row for this tenant/customer.
  // If it isn’t expired yet, return it as-is.
  const now = Date.now();
  const expiresMs = tokenRow?.expires
    ? new Date(tokenRow.expires).getTime()
    : null;

  // If we don't have an expiry stored, assume it's valid for now (MVP).
  if (!expiresMs || Number.isNaN(expiresMs)) return tokenRow;

  // Refresh 60s before expiry to avoid edge timing.
  if (expiresMs - now > 60_000) return tokenRow;

  const clientId = requireEnv("XERO_CLIENT_ID");
  const clientSecret = requireEnv("XERO_CLIENT_SECRET");

  const url = "https://identity.xero.com/connect/token";
  const form = new URLSearchParams();
  form.set("grant_type", "refresh_token");
  form.set("refresh_token", tokenRow.refresh_token);

  const { data } = await xeroApi.post(url, form.toString(), null, null, {
    headers: {
      Authorization: `Basic ${base64(`${clientId}:${clientSecret}`)}`,
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
  });

  if (!data?.access_token || !data?.refresh_token) {
    throw new Error("Failed to refresh Xero access token");
  }

  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken) {
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  }

  const expires = xeroClient.computeExpiresFromToken(data);

  // Rotate the token record for this tenant (revoke old, create new) within customer RLS txn.
  await withCustomerTxn(customerId, async (t) => {
    await XeroToken.update(
      { revoked: new Date(), revokedByIp: "system", replacedByToken: null },
      {
        where: { customerId, tenantId, revoked: null },
        transaction: t,
      }
    );

    await XeroToken.create(
      {
        access_token: data.access_token,
        refresh_token: data.refresh_token,
        scope: data.scope || tokenRow.scope || "",
        expires,
        created: new Date(),
        createdByIp: "system",
        revoked: null,
        revokedByIp: null,
        replacedByToken: null,
        customerId,
        tenantId,
      },
      { transaction: t }
    );
  });

  // Return a tokenRow-shaped object to the caller.
  return {
    ...tokenRow,
    access_token: data.access_token,
    refresh_token: data.refresh_token,
    expires,
  };
}

async function fetchPaymentsPage({ customerId, tokenRow, tenantId, page = 1 }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = `https://api.xero.com/api.xro/2.0/Payments?page=${encodeURIComponent(
    page
  )}`;

  const { data } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    }
  );

  // Xero returns { Payments: [...] }
  return {
    items: Array.isArray(data?.Payments) ? data.Payments : [],
    tokenRow: currentToken,
  };
}

async function fetchInvoiceById({ customerId, tokenRow, tenantId, invoiceId }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!invoiceId) throw new Error("Missing invoiceId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = `https://api.xero.com/api.xro/2.0/Invoices/${encodeURIComponent(
    invoiceId
  )}`;

  const { data } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    }
  );

  const invoices = Array.isArray(data?.Invoices) ? data.Invoices : [];
  return { item: invoices[0] || null, tokenRow: currentToken };
}

async function fetchContactById({ customerId, tokenRow, tenantId, contactId }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!contactId) throw new Error("Missing contactId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = `https://api.xero.com/api.xro/2.0/Contacts/${encodeURIComponent(
    contactId
  )}`;

  const { data } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    }
  );

  const contacts = Array.isArray(data?.Contacts) ? data.Contacts : [];
  return { item: contacts[0] || null, tokenRow: currentToken };
}

async function fetchBankTransactionsPage({
  customerId,
  tokenRow,
  tenantId,
  page = 1,
}) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = `https://api.xero.com/api.xro/2.0/BankTransactions?page=${encodeURIComponent(
    page
  )}`;

  const { data } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    }
  );

  return {
    items: Array.isArray(data?.BankTransactions) ? data.BankTransactions : [],
    tokenRow: currentToken,
  };
}

async function fetchOrganisationDetails({ customerId, tokenRow, tenantId }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = "https://api.xero.com/api.xro/2.0/Organisation";

  const { data } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    }
  );

  const orgs = Array.isArray(data?.Organisations) ? data.Organisations : [];

  return {
    item: orgs[0] || null,
    tokenRow: currentToken,
  };
}

module.exports = {
  connect,
  handleCallback,
  getOrganisations,
  selectOrganisations,
  removeOrganisation,
  startImport,
  getStatus,
};

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is required`);
  return v;
}

function statusKey(customerId, ptrsId) {
  return `${customerId}::${ptrsId}`;
}

async function withCustomerTxn(customerId, work) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const out = await work(t);
    await t.commit();
    return out;
  } catch (err) {
    try {
      if (!t.finished) await t.rollback();
    } catch (_) {}
    throw err;
  }
}

function getModel(name) {
  return db[name] || db.models?.[name] || null;
}

function pickModelFields(model, payload) {
  if (!model || !payload || typeof payload !== "object") return {};
  const attrs = model.rawAttributes || {};
  const out = {};
  for (const [k, v] of Object.entries(payload)) {
    if (k in attrs) out[k] = v;
  }
  return out;
}

function getFirstKey(obj, keys = []) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return null;
}

async function persistPayments({
  customerId,
  ptrsId,
  tenantId,
  payments,
  fetchedAt,
}) {
  const PtrsXeroPayment = getModel("PtrsXeroPayment");
  if (!PtrsXeroPayment) {
    // If the cache model isn't available, we still consider the fetch a success for MVP.
    return {
      inserted: 0,
      skipped: Array.isArray(payments) ? payments.length : 0,
      note: "PtrsXeroPayment model not found",
    };
  }

  const items = Array.isArray(payments) ? payments : [];

  // Guard required fields
  if (!customerId) throw new Error("persistPayments: customerId is required");
  if (!tenantId) throw new Error("persistPayments: tenantId is required");

  let inserted = 0;
  let skipped = 0;
  let loggedFirstError = false;

  // Try to store raw payloads in whatever fields the model supports.
  // We deliberately filter to existing columns to avoid hard failures while schema evolves.
  await withCustomerTxn(customerId, async (t) => {
    for (const p of items) {
      // Xero Payments API uses PaymentID; also contains Date/Amount/CurrencyRate and an Invoice reference.
      const paymentId = getFirstKey(p, [
        "PaymentID",
        "paymentId",
        "paymentID",
        "id",
      ]);

      // Best-effort mapping for model fields.
      const invoiceId = getFirstKey(p?.Invoice || p?.invoice, [
        "InvoiceID",
        "invoiceId",
        "invoiceID",
        "id",
      ]);

      const paymentDateRaw = getFirstKey(p, [
        "Date",
        "paymentDate",
        "payment_date",
      ]);
      const paymentDate = paymentDateRaw ? new Date(paymentDateRaw) : null;

      const amountRaw = getFirstKey(p, ["Amount", "amount"]);
      const amount =
        amountRaw !== null && amountRaw !== undefined && amountRaw !== ""
          ? Number(amountRaw)
          : null;

      const currency =
        getFirstKey(p, ["CurrencyRate", "currency", "Currency"]) || null;

      // These cache models REQUIRE: customerId, xeroTenantId, xeroPaymentId, rawPayload, fetchedAt.
      // Only set fields that exist on the Sequelize model via pickModelFields().
      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroPaymentId: paymentId,
        invoiceId,
        paymentDate:
          paymentDate && !Number.isNaN(paymentDate.getTime())
            ? paymentDate
            : null,
        amount: Number.isFinite(amount) ? amount : null,
        currency,
        rawPayload: p,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroPayment, candidate);

      try {
        // Prefer upsert if supported and if we have some kind of stable identifier.
        if (
          typeof PtrsXeroPayment.upsert === "function" &&
          (row.xeroPaymentId || row.xeroPaymentID || row.xeroPaymentIdRaw)
        ) {
          await PtrsXeroPayment.upsert(row, { transaction: t });
        } else {
          await PtrsXeroPayment.create(row, { transaction: t });
        }
        inserted++;
      } catch (e) {
        // If we hit uniqueness or schema/RLS issues, don't kill the whole import.
        skipped++;

        if (!loggedFirstError) {
          loggedFirstError = true;
          logger?.error?.("PTRS v2 Xero payment persist failed (first error)", {
            action: "PtrsV2XeroPersistPaymentFailed",
            customerId,
            ptrsId,
            xeroTenantId: tenantId,
            xeroPaymentId: paymentId,
            error: e?.message,
            name: e?.name,
            code: e?.original?.code || e?.parent?.code || null,
            detail: e?.original?.detail || e?.parent?.detail || null,
            constraint:
              e?.original?.constraint || e?.parent?.constraint || null,
          });
        }
      }
    }
  });

  return { inserted, skipped };
}

async function persistInvoices({
  customerId,
  ptrsId,
  tenantId,
  invoices,
  fetchedAt,
}) {
  const PtrsXeroInvoice = getModel("PtrsXeroInvoice");
  if (!PtrsXeroInvoice) {
    return {
      inserted: 0,
      skipped: Array.isArray(invoices) ? invoices.length : 0,
      note: "PtrsXeroInvoice model not found",
    };
  }

  const items = Array.isArray(invoices) ? invoices.filter(Boolean) : [];

  if (!customerId) throw new Error("persistInvoices: customerId is required");
  if (!tenantId) throw new Error("persistInvoices: tenantId is required");

  let inserted = 0;
  let skipped = 0;
  let loggedFirstError = false;

  await withCustomerTxn(customerId, async (t) => {
    for (const inv of items) {
      const xeroInvoiceId = getFirstKey(inv, [
        "InvoiceID",
        "invoiceId",
        "invoiceID",
        "id",
      ]);
      const invoiceNumber =
        getFirstKey(inv, ["InvoiceNumber", "invoiceNumber"]) || null;

      const contactId =
        getFirstKey(inv?.Contact || inv?.contact, [
          "ContactID",
          "contactId",
          "contactID",
          "id",
        ]) || null;

      const invoiceDateRaw = getFirstKey(inv, [
        "Date",
        "invoiceDate",
        "invoice_date",
      ]);
      const invoiceDate = invoiceDateRaw ? new Date(invoiceDateRaw) : null;

      const dueDateRaw = getFirstKey(inv, ["DueDate", "dueDate", "due_date"]);
      const dueDate = dueDateRaw ? new Date(dueDateRaw) : null;

      const status = getFirstKey(inv, ["Status", "status"]) || null;

      const totalRaw = getFirstKey(inv, ["Total", "total"]);
      const total =
        totalRaw !== null && totalRaw !== undefined && totalRaw !== ""
          ? Number(totalRaw)
          : null;

      const currency =
        getFirstKey(inv, ["CurrencyCode", "currency", "Currency"]) || null;

      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroInvoiceId,
        invoiceNumber,
        contactId,
        invoiceDate:
          invoiceDate && !Number.isNaN(invoiceDate.getTime())
            ? invoiceDate
            : null,
        dueDate: dueDate && !Number.isNaN(dueDate.getTime()) ? dueDate : null,
        status,
        total: Number.isFinite(total) ? total : null,
        currency,
        rawPayload: inv,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroInvoice, candidate);

      try {
        if (typeof PtrsXeroInvoice.upsert === "function" && row.xeroInvoiceId) {
          await PtrsXeroInvoice.upsert(row, { transaction: t });
        } else {
          await PtrsXeroInvoice.create(row, { transaction: t });
        }
        inserted++;
      } catch (e) {
        skipped++;

        if (!loggedFirstError) {
          loggedFirstError = true;
          logger?.error?.("PTRS v2 Xero invoice persist failed (first error)", {
            action: "PtrsV2XeroPersistInvoiceFailed",
            customerId,
            xeroTenantId: tenantId,
            xeroInvoiceId,
            error: e?.message,
            name: e?.name,
            code: e?.original?.code || e?.parent?.code || null,
            detail: e?.original?.detail || e?.parent?.detail || null,
            constraint:
              e?.original?.constraint || e?.parent?.constraint || null,
          });
        }
      }
    }
  });

  return { inserted, skipped };
}

async function persistContacts({
  customerId,
  ptrsId,
  tenantId,
  contacts,
  fetchedAt,
}) {
  const PtrsXeroContact = getModel("PtrsXeroContact");
  if (!PtrsXeroContact) {
    return {
      inserted: 0,
      skipped: Array.isArray(contacts) ? contacts.length : 0,
      note: "PtrsXeroContact model not found",
    };
  }

  const items = Array.isArray(contacts) ? contacts.filter(Boolean) : [];

  if (!customerId) throw new Error("persistContacts: customerId is required");
  if (!tenantId) throw new Error("persistContacts: tenantId is required");

  let inserted = 0;
  let skipped = 0;
  let loggedFirstError = false;

  await withCustomerTxn(customerId, async (t) => {
    for (const c of items) {
      const xeroContactId = getFirstKey(c, [
        "ContactID",
        "contactId",
        "contactID",
        "id",
      ]);
      const contactName =
        getFirstKey(c, ["Name", "contactName", "name"]) || null;
      const contactStatus =
        getFirstKey(c, ["ContactStatus", "Status", "status"]) || null;

      const isSupplierRaw = getFirstKey(c, ["IsSupplier", "isSupplier"]);
      const isSupplier =
        typeof isSupplierRaw === "boolean"
          ? isSupplierRaw
          : isSupplierRaw === "true"
            ? true
            : isSupplierRaw === "false"
              ? false
              : null;

      const abn = getFirstKey(c, ["TaxNumber", "ABN", "abn"]) || null;

      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroContactId,
        contactName,
        contactStatus,
        isSupplier,
        abn,
        rawPayload: c,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroContact, candidate);

      try {
        if (typeof PtrsXeroContact.upsert === "function" && row.xeroContactId) {
          await PtrsXeroContact.upsert(row, { transaction: t });
        } else {
          await PtrsXeroContact.create(row, { transaction: t });
        }
        inserted++;
      } catch (e) {
        skipped++;

        if (!loggedFirstError) {
          loggedFirstError = true;
          logger?.error?.("PTRS v2 Xero contact persist failed (first error)", {
            action: "PtrsV2XeroPersistContactFailed",
            customerId,
            xeroTenantId: tenantId,
            xeroContactId,
            error: e?.message,
            name: e?.name,
            code: e?.original?.code || e?.parent?.code || null,
            detail: e?.original?.detail || e?.parent?.detail || null,
            constraint:
              e?.original?.constraint || e?.parent?.constraint || null,
          });
        }
      }
    }
  });

  return { inserted, skipped };
}

async function persistBankTransactions({
  customerId,
  ptrsId,
  tenantId,
  bankTransactions,
  fetchedAt,
}) {
  const PtrsXeroBankTransaction = getModel("PtrsXeroBankTransaction");
  if (!PtrsXeroBankTransaction) {
    return {
      inserted: 0,
      skipped: Array.isArray(bankTransactions) ? bankTransactions.length : 0,
      note: "PtrsXeroBankTransaction model not found",
    };
  }

  const items = Array.isArray(bankTransactions)
    ? bankTransactions.filter(Boolean)
    : [];

  if (!customerId)
    throw new Error("persistBankTransactions: customerId is required");
  if (!tenantId)
    throw new Error("persistBankTransactions: tenantId is required");

  let inserted = 0;
  let skipped = 0;

  await withCustomerTxn(customerId, async (t) => {
    for (const bt of items) {
      const xeroBankTransactionId = getFirstKey(bt, [
        "BankTransactionID",
        "bankTransactionId",
        "bankTransactionID",
        "id",
      ]);

      const btType = getFirstKey(bt, ["Type", "type", "bankTransactionType"]);

      const btDateRaw = getFirstKey(bt, [
        "Date",
        "date",
        "bankTransactionDate",
      ]);
      const btDate = btDateRaw ? new Date(btDateRaw) : null;

      const totalRaw = getFirstKey(bt, ["Total", "total"]);
      const total =
        totalRaw !== null && totalRaw !== undefined && totalRaw !== ""
          ? Number(totalRaw)
          : null;

      const currency =
        getFirstKey(bt, ["CurrencyCode", "currency", "Currency"]) || null;

      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroBankTransactionId,
        bankTransactionType: btType || null,
        bankTransactionDate:
          btDate && !Number.isNaN(btDate.getTime()) ? btDate : null,
        total: Number.isFinite(total) ? total : null,
        currency,
        rawPayload: bt,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroBankTransaction, candidate);

      try {
        if (typeof PtrsXeroBankTransaction.upsert === "function") {
          await PtrsXeroBankTransaction.upsert(row, { transaction: t });
        } else {
          await PtrsXeroBankTransaction.create(row, { transaction: t });
        }
        inserted++;
      } catch (_) {
        skipped++;
      }
    }
  });

  return { inserted, skipped };
}

async function persistOrganisation({
  customerId,
  ptrsId,
  tenantId,
  organisation,
  fetchedAt,
}) {
  const PtrsXeroOrganisation = getModel("PtrsXeroOrganisation");
  if (!PtrsXeroOrganisation) {
    return {
      inserted: 0,
      skipped: organisation ? 1 : 0,
      note: "PtrsXeroOrganisation model not found",
    };
  }

  if (!organisation) return { inserted: 0, skipped: 0 };

  if (!customerId)
    throw new Error("persistOrganisation: customerId is required");
  if (!tenantId) throw new Error("persistOrganisation: tenantId is required");

  const xeroOrganisationId = getFirstKey(organisation, [
    "OrganisationID",
    "organisationId",
    "organisationID",
    "id",
  ]);

  const name = getFirstKey(organisation, ["Name", "name"]) || null;

  // Model requires these
  if (!xeroOrganisationId) {
    return { inserted: 0, skipped: 1, note: "Missing OrganisationID" };
  }
  if (!name) {
    return { inserted: 0, skipped: 1, note: "Missing organisation name" };
  }

  const candidate = {
    customerId,
    ptrsId,
    xeroTenantId: tenantId,
    xeroOrganisationId,
    name,
    legalName: getFirstKey(organisation, ["LegalName", "legalName"]) || null,
    registrationNumber:
      getFirstKey(organisation, ["RegistrationNumber", "registrationNumber"]) ||
      null,
    taxNumber: getFirstKey(organisation, ["TaxNumber", "taxNumber"]) || null,
    paymentTerms:
      organisation?.PaymentTerms || organisation?.paymentTerms || null,
    rawPayload: organisation,
    fetchedAt: fetchedAt || new Date(),
    deletedAt: null,
  };

  const row = pickModelFields(PtrsXeroOrganisation, candidate);

  let inserted = 0;
  let skipped = 0;

  await withCustomerTxn(customerId, async (t) => {
    try {
      if (typeof PtrsXeroOrganisation.upsert === "function") {
        await PtrsXeroOrganisation.upsert(row, { transaction: t });
      } else {
        await PtrsXeroOrganisation.create(row, { transaction: t });
      }
      inserted = 1;
    } catch (e) {
      skipped = 1;
      logger?.error?.("PTRS v2 Xero organisation persist failed", {
        action: "PtrsV2XeroPersistOrganisationFailed",
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroOrganisationId,
        error: e?.message,
        name: e?.name,
        code: e?.original?.code || e?.parent?.code || null,
        detail: e?.original?.detail || e?.parent?.detail || null,
        constraint: e?.original?.constraint || e?.parent?.constraint || null,
      });
    }
  });

  return { inserted, skipped };
}

const { Op } = require("sequelize");

async function buildRawDatasetFromXeroCache({
  customerId,
  ptrsId,
  tenantIds,
  importStartedAt,
  limit,
}) {
  const PtrsImportRaw = getModel("PtrsImportRaw");
  if (!PtrsImportRaw) {
    throw new Error(
      "PtrsImportRaw model not loaded (db.PtrsImportRaw missing)"
    );
  }

  const PtrsXeroPayment = getModel("PtrsXeroPayment");
  const PtrsXeroInvoice = getModel("PtrsXeroInvoice");
  const PtrsXeroContact = getModel("PtrsXeroContact");

  if (!PtrsXeroPayment) {
    throw new Error(
      "PtrsXeroPayment model not loaded (db.PtrsXeroPayment missing)"
    );
  }

  // 🔐 Everything must run inside customer-context txn to satisfy RLS
  return await withCustomerTxn(customerId, async (t) => {
    const whereBase = {
      customerId,
      ...(Array.isArray(tenantIds) && tenantIds.length
        ? { xeroTenantId: tenantIds }
        : {}),
    };

    const dateWhere = importStartedAt
      ? { fetchedAt: { [Op.gte]: importStartedAt } }
      : {};

    const paymentWhere = {
      ...whereBase,
      ...(PtrsXeroPayment.rawAttributes?.ptrsId ? { ptrsId } : {}),
      ...dateWhere,
    };

    const payments = await PtrsXeroPayment.findAll({
      where: paymentWhere,
      order: [["fetchedAt", "DESC"]],
      limit: limit || undefined,
      transaction: t,
    });

    const paymentRows = payments.map((p) => p.toJSON());

    const invoiceIds = Array.from(
      new Set(paymentRows.map((p) => p.invoiceId).filter(Boolean))
    );

    let invoicesById = new Map();
    if (PtrsXeroInvoice && invoiceIds.length) {
      const invoiceWhere = {
        ...whereBase,
        ...(PtrsXeroInvoice.rawAttributes?.ptrsId ? { ptrsId } : {}),
        xeroInvoiceId: invoiceIds,
        ...dateWhere,
      };

      const invoices = await PtrsXeroInvoice.findAll({
        where: invoiceWhere,
        transaction: t,
      });

      invoicesById = new Map(
        invoices.map((inv) => {
          const j = inv.toJSON();
          return [j.xeroInvoiceId, j];
        })
      );
    }

    const contactIds = Array.from(
      new Set(
        Array.from(invoicesById.values())
          .map((inv) => inv.contactId)
          .filter(Boolean)
      )
    );

    let contactsById = new Map();
    if (PtrsXeroContact && contactIds.length) {
      const contactWhere = {
        ...whereBase,
        ...(PtrsXeroContact.rawAttributes?.ptrsId ? { ptrsId } : {}),
        xeroContactId: contactIds,
        ...dateWhere,
      };

      const contacts = await PtrsXeroContact.findAll({
        where: contactWhere,
        transaction: t,
      });

      contactsById = new Map(
        contacts.map((c) => {
          const j = c.toJSON();
          return [j.xeroContactId, j];
        })
      );
    }

    // Make reruns deterministic
    await PtrsImportRaw.destroy({
      where: { customerId, ptrsId },
      transaction: t,
    });

    const rowsToInsert = [];

    for (let i = 0; i < paymentRows.length; i++) {
      const p = paymentRows[i];
      const inv = p.invoiceId ? invoicesById.get(p.invoiceId) : null;
      const c = inv?.contactId ? contactsById.get(inv.contactId) : null;

      rowsToInsert.push({
        customerId,
        ptrsId,
        rowNo: i + 1,
        data: {
          source: "xero",
          xeroTenantId: p.xeroTenantId,

          xeroPaymentId: p.xeroPaymentId,
          paymentDate: p.paymentDate || null,
          amount: p.amount ?? null,
          currency: p.currency || null,

          xeroInvoiceId: p.invoiceId || inv?.xeroInvoiceId || null,
          invoiceNumber: inv?.invoiceNumber || null,
          invoiceDate: inv?.invoiceDate || null,
          dueDate: inv?.dueDate || null,
          invoiceStatus: inv?.status || null,
          invoiceTotal: inv?.total ?? null,

          xeroContactId: inv?.contactId || c?.xeroContactId || null,
          supplierName: c?.contactName || null,
          supplierAbn: c?.abn || null,
          supplierStatus: c?.contactStatus || null,

          rawPayment: p.rawPayload || null,
          rawInvoice: inv?.rawPayload || null,
          rawContact: c?.rawPayload || null,
        },
      });
    }

    if (rowsToInsert.length) {
      await PtrsImportRaw.bulkCreate(rowsToInsert, { transaction: t });
    }

    return { insertedRows: paymentRows.length };
  });
}

// ------------------------
// OAuth + tenant selection
// ------------------------

async function connect({ customerId, ptrsId, userId }) {
  const state = Buffer.from(
    JSON.stringify({ ptrsId, customerId, ts: Date.now() })
  ).toString("base64url");

  const redirectUri = requireEnv("XERO_REDIRECT_URI");
  const authUrl = xeroClient.buildAuthUrl({ state, redirectUri });

  logger?.info?.("PTRS v2 Xero connect URL generated", {
    action: "PtrsV2XeroConnectUrl",
    customerId,
    ptrsId,
    userId,
  });

  return { ptrsId, authUrl };
}

async function handleCallback({ ptrsId, code, state }) {
  if (!code) throw new Error("Missing authorisation code");

  let parsedState = null;
  try {
    parsedState = state
      ? JSON.parse(Buffer.from(String(state), "base64url").toString("utf8"))
      : null;
  } catch (_) {
    parsedState = null;
  }

  const effectivePtrsId = ptrsId || parsedState?.ptrsId || null;
  if (!effectivePtrsId) {
    throw new Error(
      "Missing ptrsId in Xero callback. Ensure connect() was called before OAuth redirect."
    );
  }

  const customerId = parsedState?.customerId || null;
  if (!customerId) {
    throw new Error(
      "Missing customerId in Xero callback state. Ensure connect() was called before OAuth redirect."
    );
  }

  const redirectUri = requireEnv("XERO_REDIRECT_URI");

  const token = await xeroClient.exchangeAuthCodeForToken({
    code,
    redirectUri,
  });

  const connections = await xeroClient.listConnections({
    accessToken: token.access_token,
  });

  const organisations = (connections || []).map((c) => ({
    tenantId: c.tenantId,
    tenantName: c.tenantName,
  }));

  if (!organisations.length) {
    throw new Error("No Xero organisations returned from connections endpoint");
  }

  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken) {
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  }

  const expires = xeroClient.computeExpiresFromToken(token);

  await withCustomerTxn(customerId, async (t) => {
    // Multi-tenant: replace tokens PER TENANT, not by nuking everything for the customer.
    for (const org of organisations) {
      if (!org?.tenantId) continue;

      await XeroToken.update(
        { revoked: new Date(), revokedByIp: "system" },
        {
          where: { customerId, tenantId: org.tenantId, revoked: null },
          transaction: t,
        }
      );

      await XeroToken.create(
        {
          access_token: token.access_token,
          refresh_token: token.refresh_token,
          scope: token.scope || "",
          expires,
          created: new Date(),
          createdByIp: "system",
          revoked: null,
          revokedByIp: null,
          replacedByToken: null,
          customerId,
          tenantId: org.tenantId,
          // NOTE: tenantName persistence will be handled later once model supports it.
        },
        { transaction: t }
      );
    }
  });

  const frontEndBase = process.env.FRONTEND_URL || "http://localhost:3000";
  const orgParam = encodeURIComponent(JSON.stringify(organisations));
  const redirectUrl = `${frontEndBase}/v2/ptrs/xero/select?ptrsId=${encodeURIComponent(
    effectivePtrsId
  )}&organisations=${orgParam}`;

  return { redirectUrl, organisations };
}

async function getOrganisations({ customerId, ptrsId }) {
  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken) {
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  }

  const rows = await withCustomerTxn(customerId, async (t) => {
    return await XeroToken.findAll({
      where: { customerId, revoked: null },
      order: [["created", "DESC"]],
      limit: 50,
      transaction: t,
    });
  });

  const organisations = Array.from(
    new Map(
      (rows || [])
        .filter((r) => r?.tenantId)
        .map((r) => [
          r.tenantId,
          { tenantId: r.tenantId, tenantName: r.tenantName || r.tenantId },
        ])
    ).values()
  );

  return { ptrsId, status: "READY", organisations };
}

async function selectOrganisations({ customerId, ptrsId, tenantIds, userId }) {
  const selected = Array.isArray(tenantIds) ? tenantIds.filter(Boolean) : [];
  if (!selected.length) throw new Error("tenantIds is required");

  // Persist selection for this run in-memory (MVP)
  setSelectedTenantIds(customerId, ptrsId, selected);

  const PtrsXeroImport = getModel("PtrsXeroImport");
  if (PtrsXeroImport && PtrsXeroImport.rawAttributes?.selectedTenantIds) {
    await withCustomerTxn(customerId, async (t) => {
      await PtrsXeroImport.upsert(
        pickModelFields(PtrsXeroImport, {
          customerId,
          ptrsId,
          selectedTenantIds: selected,
          updatedAt: new Date(),
        }),
        { transaction: t }
      );
    });
  }

  // Multi-tenant MVP: do not revoke non-selected tenants here.
  // Selection persistence can be added later (e.g., on the PTRS run), but for now we
  // keep tokens for all connected tenants and rely on later steps to choose how to
  // import/aggregate.
  logger?.info?.("PTRS v2 Xero organisations selected", {
    action: "PtrsV2XeroOrganisationsSelect",
    customerId,
    ptrsId,
    tenantIds: selected,
    userId,
  });

  return { ptrsId, status: "READY", selectedTenantIds: selected };
}

async function removeOrganisation({ customerId, ptrsId, tenantId, userId }) {
  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken)
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");

  await withCustomerTxn(customerId, async (t) => {
    await XeroToken.update(
      { revoked: new Date(), revokedByIp: "system" },
      { where: { customerId, tenantId, revoked: null }, transaction: t }
    );
  });

  logger?.info?.("PTRS v2 Xero organisation removed", {
    action: "PtrsV2XeroOrganisationRemove",
    customerId,
    ptrsId,
    tenantId,
    userId,
  });

  return { ptrsId, status: "READY" };
}

// ------------------------
// Import + status (MVP placeholders)
// ------------------------

async function startImport({ customerId, ptrsId, userId }) {
  const importStartedAt = new Date();
  const extractLimit = getXeroExtractLimit();
  let selectedTenantIds = getSelectedTenantIds(customerId, ptrsId);

  if (!selectedTenantIds.length) {
    const PtrsXeroImport = getModel("PtrsXeroImport");
    if (PtrsXeroImport && PtrsXeroImport.rawAttributes?.selectedTenantIds) {
      const row = await withCustomerTxn(customerId, async (t) => {
        return await PtrsXeroImport.findOne({
          where: { customerId, ptrsId },
          transaction: t,
        });
      });
      const persisted = row?.selectedTenantIds;
      if (Array.isArray(persisted) && persisted.length) {
        selectedTenantIds = persisted.filter(Boolean);
        setSelectedTenantIds(customerId, ptrsId, selectedTenantIds);
      }
    }
  }

  if (!selectedTenantIds.length) {
    // Fail loudly: user must select at least one org for MVP.
    const failed = updateStatus(customerId, ptrsId, {
      status: "FAILED",
      message:
        "No organisations selected for this PTRS run. Please select an organisation first.",
      progress: { extractLimit, extractedCount: 0, tenantCount: 0 },
    });
    return failed;
  }

  // Seed status immediately so the FE can render something.
  const seed = updateStatus(customerId, ptrsId, {
    status: "RUNNING",
    message: "Starting Xero import…",
    progress: {
      extractLimit,
      extractedCount: 0,
      tenantCount: selectedTenantIds.length,
      currentTenantIndex: 0,
    },
  });

  // Fire-and-forget runner (MVP). Status is polled via /status.
  setImmediate(async () => {
    const counter = { count: 0 };
    let insertedCount = 0;

    let paymentsInserted = 0;
    let invoicesInserted = 0;
    let contactsInserted = 0;
    let bankTxInserted = 0;

    try {
      const XeroToken = db.XeroToken || db.models?.XeroToken;
      if (!XeroToken) {
        throw new Error("XeroToken model not loaded (db.XeroToken missing)");
      }

      updateStatus(customerId, ptrsId, {
        status: "RUNNING",
        message: "Import in progress…",
      });

      // Process tenants sequentially (MVP) to avoid rate-limit spikes.
      for (let i = 0; i < selectedTenantIds.length; i++) {
        const tenantId = selectedTenantIds[i];

        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message: `Importing from Xero organisation ${i + 1} of ${
            selectedTenantIds.length
          }…`,
          progress: { currentTenantIndex: i + 1 },
        });

        // Get active token row for this tenant/customer
        let tokenRow = await withCustomerTxn(customerId, async (t) => {
          return await XeroToken.findOne({
            where: { customerId, tenantId, revoked: null },
            order: [["created", "DESC"]],
            transaction: t,
          });
        });

        if (!tokenRow) {
          throw new Error(
            `No active Xero token found for selected tenantId ${tenantId}`
          );
        }

        // Persist organisation details for this tenant (needed for payer ABN/name + org display)
        try {
          const orgFetchedAt = new Date();

          const { item: organisation, tokenRow: orgTokenRow } =
            await fetchOrganisationDetails({
              customerId,
              tokenRow,
              tenantId,
            });

          // Keep using the freshest token in case auto-refresh occurred
          if (orgTokenRow) tokenRow = orgTokenRow;

          await persistOrganisation({
            customerId,
            ptrsId,
            tenantId,
            organisation,
            fetchedAt: orgFetchedAt,
          });
        } catch (e) {
          // Do not fail the whole import if org fetch/persist fails; log once per tenant
          logger?.warn?.("PTRS v2 Xero organisation fetch/persist failed", {
            action: "PtrsV2XeroPersistOrganisationFailed",
            customerId,
            ptrsId,
            tenantId,
            error: e?.message,
          });
        }

        // Refresh token if needed (expiry-aware)
        tokenRow = await refreshAccessTokenIfNeeded({
          customerId,
          tenantId,
          tokenRow,
        });

        // Fetch payments (paged) and apply global extract limit.
        const tenantPaymentItems = [];
        let paymentsEmpty = false;

        await paginateXeroApi(
          async (pageNum) => {
            const paymentPage = await fetchPaymentsPage({
              customerId,
              tokenRow,
              tenantId,
              page: pageNum,
            });
            tokenRow = paymentPage.tokenRow;
            return {
              data: { Payments: paymentPage.items },
              headers: {},
              status: 200,
            };
          },
          async (response) => {
            const pageItems = Array.isArray(response?.data?.Payments)
              ? response.data.Payments
              : [];
            if (!pageItems.length) {
              paymentsEmpty = true;
              return;
            }

            const { items: limitedItems, done } = applyExtractLimit(
              pageItems,
              extractLimit,
              counter
            );

            tenantPaymentItems.push(...limitedItems);

            if (done) paymentsEmpty = true;
          },
          {
            startPage: 1,
            // Stop when the API returns an empty page, or we hit the global cap.
            hasMoreFn: () => !paymentsEmpty,
          }
        );

        // Persist payments (best-effort)
        const payPersist = await persistPayments({
          customerId,
          ptrsId,
          tenantId,
          payments: tenantPaymentItems,
          fetchedAt: importStartedAt,
        });

        paymentsInserted += payPersist.inserted || 0;

        insertedCount += payPersist.inserted || 0;

        // Derive invoice IDs from the payments we actually kept.
        const invoiceIds = Array.from(
          new Set(
            tenantPaymentItems
              .map((p) =>
                getFirstKey(p?.Invoice || p?.invoice, [
                  "InvoiceID",
                  "invoiceId",
                  "invoiceID",
                  "id",
                ])
              )
              .filter(Boolean)
          )
        );

        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message: `Fetched ${tenantPaymentItems.length} payments (saved ${payPersist.inserted || 0}) from organisation ${i + 1} of ${
            selectedTenantIds.length
          }. Fetching related invoices/contacts…`,
          progress: {
            extractedCount: counter.count,
            insertedCount,
            lastTenantId: tenantId,
          },
        });

        // Fetch + persist invoices (deduped). This is the minimum we need to later build the PTRS dataset.
        const invoices = [];
        for (const invId of invoiceIds) {
          try {
            const invoiceRes = await fetchInvoiceById({
              customerId,
              tokenRow,
              tenantId,
              invoiceId: invId,
            });
            tokenRow = invoiceRes.tokenRow;
            const inv = invoiceRes.item;
            if (inv) invoices.push(inv);
          } catch (e) {
            // Don't kill import; record the first failure and keep going.
            logger?.warn?.("PTRS v2 Xero invoice fetch failed", {
              action: "PtrsV2XeroFetchInvoiceFailed",
              customerId,
              ptrsId,
              xeroTenantId: tenantId,
              invoiceId: invId,
              error: e?.message,
              statusCode: e?.statusCode || null,
            });
          }

          await sleep(120);
        }

        const invPersist = await persistInvoices({
          customerId,
          tenantId,
          ptrsId,
          invoices,
          fetchedAt: importStartedAt,
        });

        invoicesInserted += invPersist.inserted || 0;

        // Derive contact IDs from invoices.
        const contactIds = Array.from(
          new Set(
            invoices
              .map((inv) =>
                getFirstKey(inv?.Contact || inv?.contact, [
                  "ContactID",
                  "contactId",
                  "contactID",
                  "id",
                ])
              )
              .filter(Boolean)
          )
        );

        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message: `Saved ${payPersist.inserted || 0} payments, ${invPersist.inserted || 0} invoices. Fetching contacts…`,
          progress: {
            extractedCount: counter.count,
            insertedCount,
            lastTenantId: tenantId,
          },
        });

        const contacts = [];
        for (const cId of contactIds) {
          try {
            const contactRes = await fetchContactById({
              customerId,
              tokenRow,
              tenantId,
              contactId: cId,
            });
            tokenRow = contactRes.tokenRow;
            const c = contactRes.item;
            if (c) contacts.push(c);
          } catch (e) {
            logger?.warn?.("PTRS v2 Xero contact fetch failed", {
              action: "PtrsV2XeroFetchContactFailed",
              customerId,
              ptrsId,
              xeroTenantId: tenantId,
              contactId: cId,
              error: e?.message,
              statusCode: e?.statusCode || null,
            });
          }

          await sleep(120);
        }

        const contactPersist = await persistContacts({
          customerId,
          ptrsId,
          tenantId,
          contacts,
          fetchedAt: importStartedAt,
        });

        contactsInserted += contactPersist.inserted || 0;

        // Bank transactions (v1 parity). Paged, but bounded by extractLimit for safety.
        // For MVP we only cache a small sample and do not interpret allocations yet.
        const bankTxItems = [];
        if (extractLimit) {
          const btCounter = { count: 0 };
          let btEmpty = false;

          await paginateXeroApi(
            async (pageNum) => {
              const btPageRes = await fetchBankTransactionsPage({
                customerId,
                tokenRow,
                tenantId,
                page: pageNum,
              });
              tokenRow = btPageRes.tokenRow;
              return {
                data: { BankTransactions: btPageRes.items },
                headers: {},
                status: 200,
              };
            },
            async (response) => {
              const pageItems = Array.isArray(response?.data?.BankTransactions)
                ? response.data.BankTransactions
                : [];
              if (!pageItems.length) {
                btEmpty = true;
                return;
              }

              const { items: limitedItems, done } = applyExtractLimit(
                pageItems,
                extractLimit,
                btCounter
              );

              bankTxItems.push(...limitedItems);

              if (done) btEmpty = true;
            },
            {
              startPage: 1,
              hasMoreFn: () => !btEmpty,
            }
          );
        }

        const bankPersist = await persistBankTransactions({
          customerId,
          ptrsId,
          tenantId,
          bankTransactions: bankTxItems,
          fetchedAt: importStartedAt,
        });

        bankTxInserted += bankPersist.inserted || 0;

        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message: `Organisation ${i + 1} complete: saved ${payPersist.inserted || 0} payments, ${invPersist.inserted || 0} invoices, ${contactPersist.inserted || 0} contacts, ${bankPersist.inserted || 0} bank transactions.`,
          progress: {
            extractedCount: counter.count,
            insertedCount,
            lastTenantId: tenantId,
          },
        });

        // If we’ve hit the global cap, stop early.
        if (extractLimit && counter.count >= extractLimit) break;
      }

      updateStatus(customerId, ptrsId, {
        status: "RUNNING",
        message: "Building PTRS dataset from cached Xero records…",
        progress: {
          extractedCount: counter.count,
          insertedCount,
          paymentsInserted,
          invoicesInserted,
          contactsInserted,
          bankTxInserted,
        },
      });

      const rawBuild = await buildRawDatasetFromXeroCache({
        customerId,
        ptrsId,
        tenantIds: selectedTenantIds,
        importStartedAt,
        limit: extractLimit || null,
      });

      // Ensure the standard PTRS flow can proceed: create/update a "main" dataset row
      // even when the main input was Xero (i.e. no uploaded CSV file).
      const PtrsDataset = getModel("PtrsDataset");
      if (PtrsDataset) {
        await withCustomerTxn(customerId, async (t) => {
          const candidate = {
            customerId,
            ptrsId,
            role: "main",
            fileName: "Xero import",
            storageRef: "xero",
            rowsCount: rawBuild?.insertedRows ?? null,
            status: "uploaded",
            meta: {
              source: "xero",
              selectedTenantIds: selectedTenantIds,
              extractLimit: extractLimit ?? null,
              importedAt: importStartedAt?.toISOString?.()
                ? importStartedAt.toISOString()
                : new Date().toISOString(),
            },
            createdBy: userId || null,
            updatedBy: userId || null,
          };

          const row = pickModelFields(PtrsDataset, candidate);

          // Prefer upsert; otherwise emulate it (role is expected to be unique per ptrsId).
          if (typeof PtrsDataset.upsert === "function") {
            await PtrsDataset.upsert(row, { transaction: t });
            return;
          }

          const existing = await PtrsDataset.findOne({
            where: { customerId, ptrsId, role: "main" },
            transaction: t,
          });

          if (existing) {
            await existing.update(row, { transaction: t });
          } else {
            await PtrsDataset.create(row, { transaction: t });
          }
        });
      } else {
        logger?.warn?.(
          "PtrsDataset model not loaded; cannot create main dataset for Xero run",
          {
            action: "PtrsV2XeroMainDatasetMissing",
            customerId,
            ptrsId,
          }
        );
      }

      updateStatus(customerId, ptrsId, {
        status: "COMPLETE",
        message: `Xero import completed. Cached ${paymentsInserted} payments, ${invoicesInserted} invoices, ${contactsInserted} contacts, ${bankTxInserted} bank transactions. Built ${rawBuild.insertedRows} PTRS raw rows.`,
        progress: {
          extractedCount: counter.count,
          insertedCount,
          paymentsInserted,
          invoicesInserted,
          contactsInserted,
          bankTxInserted,
          rawRowsInserted: rawBuild.insertedRows,
        },
      });
    } catch (err) {
      const sc = err?.statusCode || err?.response?.status || null;

      let hint = "";
      if (sc === 401 || sc === 403) {
        hint =
          " Authorisation failed. Try reconnecting to Xero and re-selecting your organisation.";
      } else if (sc === 429) {
        hint =
          " Rate limited by Xero. Reduce polling and try again in a minute.";
      }

      const body = err?.responseBody;
      const bodyStr =
        body && typeof body === "object"
          ? JSON.stringify(body)
          : body
            ? String(body)
            : "";

      const extra = bodyStr ? ` Details: ${bodyStr.slice(0, 500)}` : "";

      updateStatus(customerId, ptrsId, {
        status: "FAILED",
        message: `${err?.message || "Xero import failed."}${hint}${extra}`,
        progress: { extractedCount: counter.count, insertedCount },
      });

      logger?.error?.("PTRS v2 Xero import failed", {
        action: "PtrsV2XeroImportFailed",
        customerId,
        ptrsId,
        error: err?.message,
        statusCode: sc,
        url: err?.url || null,
        method: err?.method || null,
        responseBody: err?.responseBody || null,
      });
    }
  });

  // Return the seeded RUNNING status immediately.
  return seed;
}

async function getStatus({ customerId, ptrsId }) {
  const key = statusKey(customerId, ptrsId);

  const current = statusStore.get(key);

  if (current) {
    return {
      ptrsId: current.ptrsId,
      status: current.status,
      message: current.message || null,
      progress: {
        ...(current.progress || {}),
        tenantCount:
          current.progress && typeof current.progress.tenantCount === "number"
            ? current.progress.tenantCount
            : getSelectedTenantIds(customerId, ptrsId).length,
      },
      updatedAt: current.updatedAt,
    };
  }

  return {
    ptrsId,
    status: "NOT_STARTED",
    message: null,
    progress: null,
    updatedAt: new Date().toISOString(),
  };
}
