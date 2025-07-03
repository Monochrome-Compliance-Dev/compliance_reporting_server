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
      raw: true,
    });

    let rctiCount = 0;
    let nonRctiCount = 0;
    let noticeCount = 0;
    let supplyCount = 0;
    let noneMatched = 0;

    for (const record of tcpRecords) {
      const updates = {};

      // Calculate paymentTime according to PTRS rules
      if (
        record.rcti === true &&
        record.invoiceIssueDate &&
        record.paymentDate
      ) {
        // RCTI: use invoiceIssueDate to paymentDate
        const diff =
          new Date(record.paymentDate) - new Date(record.invoiceIssueDate);
        const days = Math.round(diff / (1000 * 60 * 60 * 24) + 1);
        if (!isNaN(days)) {
          updates.paymentTime = days;
          rctiCount++;
        }
      }
      //   if (
      //     record.isRcti !== true &&
      //     record.paymentDate &&
      //     (record.invoiceIssueDate || record.invoiceReceiptDate)
      //   )
      else {
        // Non-RCTI: shorter period between issue/receipt to paymentDate
        // Where a payment is made on the same day or before an invoice is issued or received, the payment time is 0 days.
        const issueDiff = record.invoiceIssueDate
          ? new Date(record.paymentDate) - new Date(record.invoiceIssueDate)
          : Infinity;
        const receiptDiff = record.invoiceReceiptDate
          ? new Date(record.paymentDate) - new Date(record.invoiceReceiptDate)
          : Infinity;

        const minDays = Math.round(
          Math.min(issueDiff, receiptDiff) / (1000 * 60 * 60 * 24)
        );
        if (!isNaN(minDays)) {
          updates.paymentTime = minDays;
          nonRctiCount++;
        } else if (record.noticeForPaymentIssueDate && record.paymentDate) {
          const diff =
            new Date(record.paymentDate) -
            new Date(record.noticeForPaymentIssueDate);
          const days = Math.round(diff / (1000 * 60 * 60 * 24));
          console.log("Notice for payment diff:", days);
          if (!isNaN(days)) {
            updates.paymentTime = days;
            noticeCount++;
          }
        } else if (record.supplyDate && record.paymentDate) {
          const diff =
            new Date(record.paymentDate) - new Date(record.supplyDate);
          const days = Math.round(diff / (1000 * 60 * 60 * 24));
          console.log("Supply date diff:", days);
          if (!isNaN(days)) {
            updates.paymentTime = days;
            supplyCount++;
          }
        }
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

      if (Object.keys(updates).length > 0) {
        await db.Tcp.update(updates, {
          where: { id: record.id },
          transaction: t,
        });
        const refreshed = await db.Tcp.findByPk(record.id, { transaction: t });
      }
      if (Object.keys(updates).length === 0) {
        // console.log("No updates applied for record", record.id);
      }
    }
    const total = tcpRecords.length;
    const matched = rctiCount + nonRctiCount + noticeCount + supplyCount;
    noneMatched = total - matched;

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
