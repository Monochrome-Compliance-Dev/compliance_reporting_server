const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const xeroClient = require("@/v2/core/xero/xeroClient.service");

// Minimal in-process status store for dev.
// In production we should persist this (e.g., tbl_ptrs_upload or a dedicated tbl_ptrs_import_job).
const statusStore = new Map();

module.exports = {
  startImport,
  getStatus,
};

// ------------------------
// Public API
// ------------------------

async function startImport({
  customerId,
  ptrsId,
  forceRefresh,
  userId = null,
}) {
  const key = statusKey(customerId, ptrsId);

  // Prevent double-start.
  const existing = statusStore.get(key);
  if (
    existing &&
    ["RUNNING", "STARTED", "IN_PROGRESS"].includes(
      String(existing.status || "").toUpperCase()
    )
  ) {
    return existing;
  }

  const started = {
    ptrsId,
    status: "STARTED",
    message: "Xero import queued",
    progress: null,
    updatedAt: new Date().toISOString(),
  };

  statusStore.set(key, started);

  // Kick the work off asynchronously so the HTTP request returns immediately.
  setImmediate(async () => {
    logger?.info?.("PTRS v2 Xero import: job started", {
      action: "PtrsV2XeroImportJobStart",
      customerId,
      ptrsId,
      forceRefresh: Boolean(forceRefresh),
      userId,
    });

    try {
      statusStore.set(key, {
        ...started,
        status: "RUNNING",
        message: "Fetching Xero data",
        updatedAt: new Date().toISOString(),
      });

      const tenantId = await xeroClient.getDefaultTenantForCustomer(customerId);
      const { periodStart, periodEnd } =
        await xeroClient.getReportingPeriodForPtrs({
          customerId,
          ptrsId,
        });

      // 1) Fetch/cache from Xero using v2 client
      await fetchAndCacheFromXeroV2({
        customerId,
        tenantId,
        ptrsId,
        periodStart,
        periodEnd,
        forceRefresh: Boolean(forceRefresh),
        userId,
        onProgress: (message, progress) => {
          statusStore.set(key, {
            ptrsId,
            status: "RUNNING",
            message,
            progress: progress ?? null,
            updatedAt: new Date().toISOString(),
          });
        },
      });

      // 2) Build PTRS main dataset rows into tbl_ptrs_import_raw
      statusStore.set(key, {
        ptrsId,
        status: "RUNNING",
        message: "Building PTRS import rows",
        progress: null,
        updatedAt: new Date().toISOString(),
      });

      const built = await buildPtrsImportRawFromCache({
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
      });

      logger?.info?.("PTRS v2 Xero import: job completed", {
        action: "PtrsV2XeroImportJobComplete",
        customerId,
        ptrsId,
        rowsInserted: built?.rowsInserted ?? null,
      });

      statusStore.set(key, {
        ptrsId,
        status: "COMPLETE",
        message: `Import complete (${built.rowsInserted} rows ready for linking)`,
        progress: null,
        updatedAt: new Date().toISOString(),
      });
    } catch (err) {
      statusStore.set(key, {
        ptrsId,
        status: "FAILED",
        message: err?.message || "Xero import failed",
        progress: null,
        updatedAt: new Date().toISOString(),
      });

      logger?.error?.("PTRS v2 Xero import: job failed", {
        action: "PtrsV2XeroImportJobFailed",
        customerId,
        ptrsId,
        error: err?.message,
      });
    }
  });

  return started;
}

async function getStatus({ customerId, ptrsId }) {
  const key = statusKey(customerId, ptrsId);
  return (
    statusStore.get(key) || {
      ptrsId,
      status: "NOT_STARTED",
      message: null,
      progress: null,
      updatedAt: new Date().toISOString(),
    }
  );
}

// ------------------------
// Internals
// ------------------------

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

async function fetchAndCacheFromXeroV2({
  customerId,
  tenantId,
  periodStart,
  periodEnd,
  forceRefresh, // currently advisory
  userId,
  onProgress,
}) {
  const fetchedAt = new Date();

  // Contacts
  onProgress("Fetching Xero contacts", 0);
  const contacts = await xeroClient.listContactsForTenant({
    customerId,
    tenantId,
  });
  await upsertContacts({
    customerId,
    xeroTenantId: tenantId,
    fetchedAt,
    contacts,
    userId,
  });

  // Invoices/Bills (Accounts Payable) - optional enrichment
  onProgress("Fetching Xero AP invoices/bills", 30);
  try {
    const invoices = await xeroClient.listApInvoicesForTenantAndPeriod({
      customerId,
      tenantId,
      periodStart,
      periodEnd,
    });
    await upsertInvoices({
      customerId,
      xeroTenantId: tenantId,
      fetchedAt,
      invoices,
      userId,
    });
  } catch (e) {
    onProgress("Skipping invoices (AP invoice fetch not available)", 35);
  }

  // Payments
  onProgress("Fetching Xero payments", 60);
  const payments = await xeroClient.listPaymentsForTenantAndPeriod({
    customerId,
    tenantId,
    periodStart,
    periodEnd,
  });
  await upsertPayments({
    customerId,
    xeroTenantId: tenantId,
    fetchedAt,
    payments,
    userId,
  });

  onProgress("Xero cache updated", 90);
}

async function upsertContacts({
  customerId,
  xeroTenantId,
  fetchedAt,
  contacts,
  userId,
}) {
  const rows = (contacts || [])
    .map((c) => ({
      customerId,
      xeroTenantId,
      xeroContactId: c?.ContactID || c?.contactId || c?.id,
      contactName: c?.Name || c?.name || null,
      contactStatus: c?.ContactStatus || c?.status || null,
      isSupplier: c?.IsSupplier ?? c?.isSupplier ?? null,
      abn: c?.TaxNumber || c?.taxNumber || c?.ABN || c?.abn || null,
      rawPayload: c,
      fetchedAt,
      ...(db.PtrsXeroContact?.rawAttributes?.updatedBy
        ? { updatedBy: userId }
        : {}),
    }))
    .filter((r) => r.xeroContactId);

  if (!rows.length) return;

  await withCustomerTxn(customerId, (t) =>
    db.PtrsXeroContact.bulkCreate(rows, {
      transaction: t,
      updateOnDuplicate: [
        "contactName",
        "contactStatus",
        "isSupplier",
        "abn",
        "rawPayload",
        "fetchedAt",
        "updatedAt",
        ...(db.PtrsXeroContact?.rawAttributes?.updatedBy ? ["updatedBy"] : []),
      ],
    })
  );
}

async function upsertInvoices({
  customerId,
  xeroTenantId,
  fetchedAt,
  invoices,
  userId,
}) {
  const rows = (invoices || [])
    .map((i) => ({
      customerId,
      xeroTenantId,
      xeroInvoiceId: i?.InvoiceID || i?.invoiceId || i?.id,
      invoiceNumber: i?.InvoiceNumber || i?.invoiceNumber || null,
      contactId: i?.Contact?.ContactID || i?.contactId || null,
      invoiceDate: i?.Date || i?.invoiceDate || null,
      dueDate: i?.DueDate || i?.dueDate || null,
      status: i?.Status || i?.status || null,
      total: i?.Total ?? i?.total ?? null,
      currency: i?.CurrencyCode || i?.currency || null,
      rawPayload: i,
      fetchedAt,
      ...(db.PtrsXeroInvoice?.rawAttributes?.updatedBy
        ? { updatedBy: userId }
        : {}),
    }))
    .filter((r) => r.xeroInvoiceId);

  if (!rows.length) return;

  await withCustomerTxn(customerId, (t) =>
    db.PtrsXeroInvoice.bulkCreate(rows, {
      transaction: t,
      updateOnDuplicate: [
        "invoiceNumber",
        "contactId",
        "invoiceDate",
        "dueDate",
        "status",
        "total",
        "currency",
        "rawPayload",
        "fetchedAt",
        "updatedAt",
        ...(db.PtrsXeroInvoice?.rawAttributes?.updatedBy ? ["updatedBy"] : []),
      ],
    })
  );
}

async function upsertPayments({
  customerId,
  xeroTenantId,
  fetchedAt,
  payments,
  userId,
}) {
  const rows = (payments || [])
    .map((p) => ({
      customerId,
      xeroTenantId,
      xeroPaymentId: p?.PaymentID || p?.paymentId || p?.id,
      invoiceId: p?.Invoice?.InvoiceID || p?.invoiceId || null,
      paymentDate: p?.Date || p?.paymentDate || null,
      amount: p?.Amount ?? p?.amount ?? null,
      currency: p?.CurrencyRate ? null : p?.CurrencyCode || p?.currency || null,
      rawPayload: p,
      fetchedAt,
      ...(db.PtrsXeroPayment?.rawAttributes?.updatedBy
        ? { updatedBy: userId }
        : {}),
    }))
    .filter((r) => r.xeroPaymentId);

  if (!rows.length) return;

  await withCustomerTxn(customerId, (t) =>
    db.PtrsXeroPayment.bulkCreate(rows, {
      transaction: t,
      updateOnDuplicate: [
        "invoiceId",
        "paymentDate",
        "amount",
        "currency",
        "rawPayload",
        "fetchedAt",
        "updatedAt",
        ...(db.PtrsXeroPayment?.rawAttributes?.updatedBy ? ["updatedBy"] : []),
      ],
    })
  );
}

async function buildPtrsImportRawFromCache({
  customerId,
  ptrsId,
  xeroTenantId,
}) {
  const payments = await withCustomerTxn(customerId, (t) =>
    db.PtrsXeroPayment.findAll({
      where: { customerId, xeroTenantId },
      order: [["paymentDate", "ASC"]],
      limit: 50000,
      transaction: t,
    })
  );

  const ImportRaw = db.PtrsImportRaw;

  if (!ImportRaw) {
    throw new Error(
      "PTRS import raw model not found in db. Expected something like db.PtrsImportRaw. Check your v2 models loader and update this mapping."
    );
  }

  // Clear existing raw import rows for this run before rebuild (idempotent).
  await withCustomerTxn(customerId, (t) =>
    ImportRaw.destroy({
      where: { ptrsId, customerId },
      transaction: t,
    })
  );

  const rows = payments.map((p, idx) => ({
    customerId,
    ptrsId,
    rowNo: idx + 1,
    data: paymentToRowObject(p),
  }));

  const chunkSize = 2000;
  let inserted = 0;

  await withCustomerTxn(customerId, async (t) => {
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      // eslint-disable-next-line no-await-in-loop
      await ImportRaw.bulkCreate(chunk, { transaction: t });
      inserted += chunk.length;
    }
  });

  return { rowsInserted: inserted };
}

function paymentToRowObject(paymentModel) {
  const raw = paymentModel?.rawPayload || {};

  return {
    "Xero Payment ID": paymentModel.xeroPaymentId,
    "Invoice ID": paymentModel.invoiceId || null,
    "Payment Date": paymentModel.paymentDate || raw?.Date || null,
    Amount: paymentModel.amount || raw?.Amount || null,
    Currency: paymentModel.currency || raw?.CurrencyCode || null,
    "Contact ID":
      raw?.Invoice?.Contact?.ContactID || raw?.Contact?.ContactID || null,
    Reference: raw?.Reference || null,
  };
}
