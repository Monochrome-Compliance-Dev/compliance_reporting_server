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

const DAY_MS = 24 * 60 * 60 * 1000;

// Returns integer day difference or null if inputs invalid.
// If inclusive is true, adds 1 day when diff is non-negative (for RCTI rule).
// If clampZero is true, negative values are clamped to 0 (non-RCTI rule).
function dayDiff(
  endDate,
  startDate,
  { inclusive = false, clampZero = false } = {}
) {
  if (!endDate || !startDate) return null;
  const end = new Date(endDate);
  const start = new Date(startDate);
  if (isNaN(end) || isNaN(start)) return null;
  let days = Math.round((end - start) / DAY_MS);
  if (inclusive && days >= 0) days += 1;
  if (clampZero && days < 0) days = 0;
  return Number.isFinite(days) ? days : null;
}

async function processTcpMetrics(ptrsId, clientId) {
  const t = await beginTransactionWithClientContext(clientId);

  try {
    // Validate ptrs belongs to client
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, clientId },
      transaction: t,
    });

    if (!ptrs) {
      throw new Error(`Ptrs ${ptrsId} not found for client ${clientId}`);
    }

    const tcpRecords = await db.Tcp.findAll({
      where: { ptrsId, clientId, isTcp: true },
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
        // RCTI: inclusive days between invoiceIssueDate and paymentDate
        const days = dayDiff(record.paymentDate, record.invoiceIssueDate, {
          inclusive: true,
          clampZero: true,
        });
        if (Number.isFinite(days)) {
          updates.paymentTime = days;
          rctiCount++;
        }
      } else {
        // Non-RCTI: choose the shortest valid path to payment
        // Rule: same-day or pre-issue/receipt payments -> 0 days.
        const candidates = [];
        const issueDays = dayDiff(record.paymentDate, record.invoiceIssueDate, {
          clampZero: true,
        });
        if (Number.isFinite(issueDays)) candidates.push(issueDays);
        const receiptDays = dayDiff(
          record.paymentDate,
          record.invoiceReceiptDate,
          { clampZero: true }
        );
        if (Number.isFinite(receiptDays)) candidates.push(receiptDays);

        if (candidates.length > 0) {
          const minDays = Math.min(...candidates);
          updates.paymentTime = minDays;
          nonRctiCount++;
        } else {
          // Fallback paths
          const noticeDays = dayDiff(
            record.paymentDate,
            record.noticeForPaymentIssueDate,
            { clampZero: true }
          );
          if (Number.isFinite(noticeDays)) {
            updates.paymentTime = noticeDays;
            noticeCount++;
          } else {
            const supplyDays = dayDiff(record.paymentDate, record.supplyDate, {
              clampZero: true,
            });
            if (Number.isFinite(supplyDays)) {
              updates.paymentTime = supplyDays;
              supplyCount++;
            }
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

      // console.debug("processTcpMetrics updates", updates);

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
      ptrsId,
      clientId,
      error,
    });
    throw error;
  }
}

module.exports = { processTcpMetrics };
