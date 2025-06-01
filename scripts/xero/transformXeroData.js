const { transformInvoices } = require("./transformInvoices");
const fs = require("fs");
const path = require("path");

// Load the raw Xero JSON dump
const xeroDataPath = path.join(__dirname, "../docs/xeroApiDump.json");
const xeroData = JSON.parse(fs.readFileSync(xeroDataPath, "utf-8"));

// Field mapping to match PTRS fields to Xero fields
// See src/features/reports/ptrs/fieldMapping.js for PTRS field names
// NOTE: If mapping is not direct, use placeholder or comment for further logic
const fieldMapping = {
  // Payer/Payee
  payerEntityName: (invoice) => "PAYER_ENTITY_NAME_PLACEHOLDER", // No direct mapping in Xero sample; set as placeholder
  payerEntityAbn: (invoice) => "PAYER_ENTITY_ABN_PLACEHOLDER", // No direct mapping; placeholder
  payerEntityAcnArbn: (invoice) => "PAYER_ENTITY_ACN_ARBN_PLACEHOLDER", // No direct mapping; placeholder
  payeeEntityName: (invoice) => invoice.Contact?.Name || "",
  payeeEntityAbn: (invoice) => "PAYEE_ENTITY_ABN_PLACEHOLDER", // No direct mapping; placeholder
  payeeEntityAcnArbn: (invoice) => "PAYEE_ENTITY_ACN_ARBN_PLACEHOLDER", // No direct mapping; placeholder
  // Payment
  paymentAmount: (invoice) => {
    // If multiple payments, sum or take first? Here, sum all payment amounts
    if (Array.isArray(invoice.Payments) && invoice.Payments.length > 0) {
      return invoice.Payments.reduce((sum, p) => sum + (p.Amount || 0), 0);
    }
    return 0;
  },
  description: (invoice) => invoice.Reference || "", // Could also use LineItems[0]?.Description if needed
  supplyDate: (invoice) => {
    // No direct mapping; could use invoice.DateString or leave blank
    // Placeholder logic: use invoice.DateString
    return invoice.DateString || "";
  },
  paymentDate: (invoice) => {
    // Use first payment date if exists
    if (Array.isArray(invoice.Payments) && invoice.Payments.length > 0) {
      // Xero format: "/Date(1742428800000+0000)/"
      const dateStr = invoice.Payments[0].DateString;
      if (dateStr) return dateStr;
      if (invoice.Payments[0].Date) {
        // Try to parse /Date(....)/
        const match = /\/Date\((\d+)/.exec(invoice.Payments[0].Date);
        if (match) return new Date(Number(match[1])).toISOString().slice(0, 10);
      }
    }
    return "";
  },
  // Contract
  contractPoReferenceNumber: (invoice) => "", // No direct mapping; placeholder
  contractPoPaymentTerms: (invoice) => "", // No direct mapping; placeholder
  // Notice
  noticeForPaymentIssueDate: (invoice) => "", // No direct mapping; placeholder
  noticeForPaymentTerms: (invoice) => "", // No direct mapping; placeholder
  // Invoice
  invoiceReferenceNumber: (invoice) => invoice.InvoiceNumber || "",
  invoiceIssueDate: (invoice) => invoice.DateString || "",
  invoiceReceiptDate: (invoice) => "", // No direct mapping; placeholder
  invoiceAmount: (invoice) => invoice.Total || 0,
  invoicePaymentTerms: (invoice) => "", // No direct mapping; placeholder
  invoiceDueDate: (invoice) => invoice.DueDateString || "",
};

// Only take first 5 invoices for testing
const invoicesToTest = xeroData.invoices.slice(0, 5);

// Transform function using fieldMapping
const transformedData = transformInvoices(invoicesToTest, {
  contactMap: xeroData.contactMap || {},
});

// Log out the first 5 records from each of the invoices, payments, and purchaseOrders arrays
console.log("========== RAW XERO DATA DUMP ==========");
console.log("----- First 5 xeroData.invoices (RAW) -----");
console.log(JSON.stringify(xeroData.invoices.slice(0, 5), null, 2));

if (Array.isArray(xeroData.payments)) {
  console.log("----- First 5 xeroData.payments (RAW) -----");
  console.log(JSON.stringify(xeroData.payments.slice(0, 5), null, 2));
} else {
  console.log("No payments array found in xeroData.");
}

if (Array.isArray(xeroData.purchaseOrders)) {
  console.log("----- First 5 xeroData.purchaseOrders (RAW) -----");
  console.log(JSON.stringify(xeroData.purchaseOrders.slice(0, 5), null, 2));
} else {
  console.log("No purchaseOrders array found in xeroData.");
}

console.log("========== END RAW XERO DATA DUMP ==========");

console.log("========== TRANSFORMED DATA ==========");
console.log("Transformed Data (first 5 invoices):");
console.log(JSON.stringify(transformedData, null, 2));
console.log("========== END TRANSFORMED DATA ==========");

// Write transformedData to CSV using csv-writer
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const csvPath = path.join(__dirname, "transformedData.csv");

// Use the order of the keys in the first transformed object for header
const csvHeaders =
  transformedData.length > 0
    ? Object.keys(transformedData[0]).map((key) => ({ id: key, title: key }))
    : [];

const csvWriter = createCsvWriter({
  path: csvPath,
  header: csvHeaders,
});

csvWriter
  .writeRecords(transformedData)
  .then(() => {
    console.log(`Transformed data written to ${csvPath}`);
  })
  .catch((err) => {
    console.error("Error writing CSV:", err);
  });
