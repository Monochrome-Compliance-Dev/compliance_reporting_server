// Map of Modern Slavery entity types to fields that should be tracked for audit diffs
const auditFieldConfig = {
  mstraining: ["employeeName", "department", "completed", "completedAt"],
  mssupplierRisk: ["name", "risk", "country", "reviewed"],
  msgrievance: ["description", "status", "reportedAt", "updatedBy"],
  msinterviewResponse: ["section", "question", "answer", "updatedBy"],
  msreportingPeriod: ["name", "startDate", "endDate", "status", "updatedBy"],
};

module.exports = {
  auditFieldConfig,
};
