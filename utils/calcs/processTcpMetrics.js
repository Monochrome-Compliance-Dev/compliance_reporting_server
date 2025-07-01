const db = require("../../db/database");
const { logger } = require("../../helpers/logger");
// const { sequelize } = require("../../db/database");
const {
  beginTransactionWithClientContext,
} = require("../../helpers/setClientIdRLS");

function appendComment(existing, addition) {
  if (!existing) return addition;
  if (existing.includes(addition)) return existing;
  return `${existing} | ${addition}`;
}

async function processTcpMetrics(reportId, clientId) {
  const t = await beginTransactionWithClientContext(clientId);

  try {
    // Validate report belongs to client
    const report = await db.Report.findOne({
      where: { id: reportId, clientId },
      transaction: t,
    });

    if (!report) {
      throw new Error(`Report ${reportId} not found for client ${clientId}`);
    }

    const tcpRecords = await db.Tcp.findAll({
      where: { reportId, clientId, isTcp: true },
      transaction: t,
    });
    // console.log("tcpRecords: ", tcpRecords);

    for (const record of tcpRecords) {
      console.log("Evaluating record:", {
        id: record.id,
        isRcti: record.isRcti,
        invoiceIssueDate: record.invoiceIssueDate,
        invoiceReceiptDate: record.invoiceReceiptDate,
        noticeForPaymentIssueDate: record.noticeForPaymentIssueDate,
        supplyDate: record.supplyDate,
        paymentDate: record.paymentDate,
        invoiceDueDate: record.invoiceDueDate,
        invoicePaymentTerms: record.invoicePaymentTerms,
        noticeForPaymentTerms: record.noticeForPaymentTerms,
        contractPoPaymentTerms: record.contractPoPaymentTerms,
        paymentAmount: record.paymentAmount,
        invoiceAmount: record.invoiceAmount,
      });

      const updates = {};

      // Calculate paymentTime according to PTRS rules
      if (
        record.isRcti === true &&
        record.invoiceIssueDate &&
        record.paymentDate
      ) {
        // RCTI: use invoiceIssueDate to paymentDate
        const diff =
          new Date(record.paymentDate) - new Date(record.invoiceIssueDate);
        const days = Math.round(diff / (1000 * 60 * 60 * 24));
        console.log("RCTI diff (issue to payment):", days);
        if (!isNaN(days)) updates.paymentTime = days;
      } else if (
        record.isRcti === false &&
        record.paymentDate &&
        (record.invoiceIssueDate || record.invoiceReceiptDate)
      ) {
        // Non-RCTI: shorter period between issue/receipt to paymentDate
        const issueDiff = record.invoiceIssueDate
          ? new Date(record.paymentDate) - new Date(record.invoiceIssueDate)
          : Infinity;
        const receiptDiff = record.invoiceReceiptDate
          ? new Date(record.paymentDate) - new Date(record.invoiceReceiptDate)
          : Infinity;

        const minDays = Math.round(
          Math.min(issueDiff, receiptDiff) / (1000 * 60 * 60 * 24)
        );
        console.log("Non-RCTI diffs:", { issueDiff, receiptDiff, minDays });
        if (!isNaN(minDays)) updates.paymentTime = minDays;
      } else if (record.noticeForPaymentIssueDate && record.paymentDate) {
        const diff =
          new Date(record.paymentDate) -
          new Date(record.noticeForPaymentIssueDate);
        const days = Math.round(diff / (1000 * 60 * 60 * 24));
        console.log("Notice for payment diff:", days);
        if (!isNaN(days)) updates.paymentTime = days;
      } else if (record.supplyDate && record.paymentDate) {
        const diff = new Date(record.paymentDate) - new Date(record.supplyDate);
        const days = Math.round(diff / (1000 * 60 * 60 * 24));
        console.log("Supply date diff:", days);
        if (!isNaN(days)) updates.paymentTime = days;
      }

      // Calculate paymentTerm
      let paymentTerm = null;
      const invoiceIssueDate = record.invoiceIssueDate;
      const invoiceDueDate = record.invoiceDueDate;

      if (
        invoiceIssueDate &&
        invoiceDueDate &&
        !isNaN(Date.parse(invoiceIssueDate)) &&
        !isNaN(Date.parse(invoiceDueDate))
      ) {
        const issue = new Date(invoiceIssueDate);
        const due = new Date(invoiceDueDate);

        if (due >= issue) {
          paymentTerm = Math.round((due - issue) / (1000 * 60 * 60 * 24)) + 1; // Inclusive of both dates
        } else {
          logger.warn("Invalid Payment Term: Due date before issue date", {
            id: record.id,
            invoiceIssueDate,
            invoiceDueDate,
          });
          paymentTerm = null;
          record.explanatoryComments1 = appendComment(
            record.explanatoryComments1,
            "Invalid term: due date before issue date"
          );
          updates.explanatoryComments1 = record.explanatoryComments1;
        }
      }

      if (paymentTerm !== null) {
        updates.paymentTerm = paymentTerm;
      }

      // Fallback for paymentTerm if still null
      if (updates.paymentTerm == null) {
        const altTerm =
          Number(record.invoicePaymentTerms) ||
          Number(record.noticeForPaymentTerms) ||
          Number(record.contractPoPaymentTerms);

        if (!isNaN(altTerm) && altTerm > 0) {
          updates.paymentTerm = altTerm;
          updates.explanatoryComments1 = appendComment(
            record.explanatoryComments1,
            "Used fallback term field"
          );
        } else {
          updates.paymentTerm = 31;
          updates.explanatoryComments1 = appendComment(
            record.explanatoryComments1,
            "Fallback to default 31 day term"
          );
        }
      }

      // Calculate partialPayment
      if (
        record.paymentAmount != null &&
        record.invoiceAmount != null &&
        Number(record.paymentAmount) < Number(record.invoiceAmount)
      ) {
        updates.partialPayment = true;
      } else if (record.paymentAmount != null && record.invoiceAmount != null) {
        updates.partialPayment = false;
      }
      console.log("Partial payment check:", {
        paymentAmount: record.paymentAmount,
        invoiceAmount: record.invoiceAmount,
        isPartial: updates.partialPayment,
      });

      if (Object.keys(updates).length > 0) {
        console.log("Before update:", {
          id: record.id,
          currentTerm: record.paymentTerm,
          newTerm: updates.paymentTerm,
        });
        await record.update(updates, { transaction: t });
        const refreshed = await db.Tcp.findByPk(record.id, { transaction: t });
        console.log("After update:", {
          id: refreshed.id,
          paymentTerm: refreshed.paymentTerm,
        });
      }
      if (Object.keys(updates).length === 0) {
        console.log("No updates applied for record", record.id);
      }
    }

    await t.commit();
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Error processing TCP metrics", {
      action: "ProcessTcpMetrics",
      reportId,
      clientId,
      error,
    });
    throw error;
  }
}

module.exports = { processTcpMetrics };
