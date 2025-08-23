// Map of Modern Slavery entity types to fields that should be tracked for audit diffs
const auditFieldConfig = {
  mstraining: ["employeeName", "department", "completed", "completedAt"],
  mssupplierRisk: ["name", "risk", "country", "reviewed"],
  msgrievance: ["description", "status", "reportedAt", "updatedBy"],
  msinterviewResponse: ["section", "question", "answer", "updatedBy"],
  msreportingPeriod: ["name", "startDate", "endDate", "status", "updatedBy"],
  invoice: [
    "billingType",
    "customerId",
    "partnerId",
    "reportingPeriodId",
    "issuedAt",
    "totalAmount",
    "status",
    "updatedBy",
  ],
  invoiceLine: [
    "invoiceId",
    "description",
    "amount",
    "module",
    "relatedRecordId",
    "productId",
    "createdBy",
    "updatedBy",
  ],
  product: [
    "name",
    "code",
    "type",
    "module",
    "amount",
    "active",
    "createdBy",
    "updatedBy",
  ],
  partner: [
    "name",
    "contactName",
    "contactEmail",
    "discountRate",
    "createdBy",
    "updatedBy",
  ],
};

module.exports = {
  auditFieldConfig,
};
