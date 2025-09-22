const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
const db = require("../db/database");

const SCHEMA = process.env.DB_SCHEMA || "public";

module.exports = {
  startIngest,
  getIngestJob,
  listPtrsRows,
  listPtrsErrors,
};

/**
 * Create an ingest job in queued state.
 * Controller is responsible for actually kicking the worker.
 */
async function startIngest({
  filePath,
  customerId,
  ptrsId,
  originalName,
  sizeBytes,
  format = "csv",
  userId,
}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    if (!filePath) throw { status: 400, message: "filePath is required" };
    if (!ptrsId) throw { status: 400, message: "ptrsId is required" };

    const { nanoid } = await import("nanoid");
    const jobId = nanoid();

    const sql = `
      INSERT INTO ${SCHEMA}."tbl_ingest_job"
        ("id","customerId","ptrsId","s3key","originalName","sizeBytes","format","status","rowsProcessed","rowsValid","rowsErrored","startedAt","updatedAt","createdBy","updatedBy")
      VALUES
        (:id,:customerId,:ptrsId,:s3key,:originalName,:sizeBytes,:format,'queued',0,0,0, NOW(), NOW(), :createdBy, :updatedBy)
    `;

    await db.sequelize.query(sql, {
      replacements: {
        id: jobId,
        customerId,
        ptrsId,
        s3key: filePath,
        originalName: originalName || filePath,
        sizeBytes: sizeBytes || 0,
        format,
        createdBy: userId || "system",
        updatedBy: userId || "system",
      },
      transaction: t,
    });

    await t.commit();
    return { id: jobId };
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

/** Fetch a single ingest job (RLS-scoped by transaction). */
async function getIngestJob({ id, customerId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const sql = `
      SELECT "id","customerId","ptrsId","s3key","originalName","sizeBytes","format","status","rowsProcessed","rowsValid","rowsErrored","startedAt","finishedAt","lastError","updatedAt","createdAt","createdBy","updatedBy"
      FROM ${SCHEMA}."tbl_ingest_job"
      WHERE "id" = :id
      LIMIT 1
    `;
    const [rows] = await db.sequelize.query(sql, {
      replacements: { id },
      transaction: t,
      plain: false,
    });
    await t.commit();
    const row = Array.isArray(rows) ? rows[0] : rows;
    if (!row) throw { status: 404, message: "Ingest job not found" };
    return row;
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

/** List valid TCP rows for a PTRS dataset with keyset pagination. */
async function listPtrsRows({ ptrsId, customerId, limit = 100, cursor }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const lim = clampLimit(limit);
    const hasCursor = cursor != null && cursor !== "";
    const sql = `
      SELECT
        "id","payerEntityName","payerEntityAbn","payerEntityAcnArbn",
        "payeeEntityName","payeeEntityAbn","payeeEntityAcnArbn",
        "paymentAmount","description","transactionType","isReconciled",
        "supplyDate","paymentDate","contractPoReferenceNumber","contractPoPaymentTerms",
        "noticeForPaymentIssueDate","noticeForPaymentTerms","invoiceReferenceNumber",
        "invoiceIssueDate","invoiceReceiptDate","invoiceAmount","invoicePaymentTerms",
        "invoiceDueDate","accountCode","isTcp","tcpExclusionComment","peppolEnabled",
        "rcti","creditCardPayment","creditCardNumber","partialPayment","paymentTerm",
        "excludedTcp","explanatoryComments1","isSb","paymentTime","explanatoryComments2",
        "source","createdBy","updatedBy","customerId","ptrsId","createdAt","updatedAt","deletedAt","deletedBy"
      FROM ${SCHEMA}."tbl_tcp"
      WHERE "customerId" = :customerId AND "ptrsId" = :ptrsId
        ${hasCursor ? 'AND "id" > :cursor' : ""}
      ORDER BY "id"
      LIMIT :limit
    `;
    const [rows] = await db.sequelize.query(sql, {
      replacements: { customerId, ptrsId, cursor, limit: lim },
      transaction: t,
    });
    await t.commit();
    const nextCursor =
      rows && rows.length === lim ? rows[rows.length - 1].id : null;
    return { items: rows || [], nextCursor };
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

/** List TCP error rows for a PTRS dataset with keyset pagination. */
async function listPtrsErrors({ ptrsId, customerId, limit = 100, cursor }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const lim = clampLimit(limit);
    const hasCursor = cursor != null && cursor !== "";
    const sql = `
      SELECT
        "id",
        "payerEntityName","payerEntityAbn",
        "payeeEntityName","payeeEntityAbn",
        "paymentAmount","paymentDate",
        "invoiceAmount","invoiceDueDate",
        "errorReason",
        "createdBy","updatedBy","customerId","ptrsId","createdAt","updatedAt"
      FROM ${SCHEMA}."tbl_tcp_error"
      WHERE "customerId" = :customerId AND "ptrsId" = :ptrsId
        ${hasCursor ? 'AND "id" > :cursor' : ""}
      ORDER BY "id"
      LIMIT :limit
    `;
    const [rows] = await db.sequelize.query(sql, {
      replacements: { customerId, ptrsId, cursor, limit: lim },
      transaction: t,
    });
    await t.commit();
    const nextCursor =
      rows && rows.length === lim ? rows[rows.length - 1].id : null;
    return { items: rows || [], nextCursor };
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

function clampLimit(x) {
  const n = Math.max(1, Math.min(1000, Number(x) || 100));
  return n;
}
