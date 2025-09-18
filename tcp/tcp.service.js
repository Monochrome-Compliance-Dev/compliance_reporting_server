const db = require("../db/database");
const ptrsService = require("../ptrs/ptrs.service");
const { tcpBulkImportSchema } = require("./tcp.validator");
const { sequelize } = require("../db/database");
const { Op } = require("sequelize");
const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
const dateNormaliser = require("../helpers/dateNormaliser");
const parseDateLike =
  (dateNormaliser && dateNormaliser.parseDateLike) || dateNormaliser;

module.exports = {
  /**
   * Retrieve all TCP records.
   * @param {Object} params - Parameters object.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Array>} List of TCP records.
   */
  getAll,

  /**
   * Retrieve TCP records by PTRS ID.
   * @param {Object} params - Parameters object containing customerId and ptrsId.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Array>} List of TCP records for the given ptrsId.
   */
  getByPtrsId,

  /**
   * Update the isSb flag to false for TCP records matching ptrsId and payeeEntityAbn.
   * @param {Object} params - Parameters object containing ptrsId and payeeEntityAbn.
   * @param {Object} [options] - Query options.
   * @returns {Promise<void>}
   */
  sbiUpdate,

  /**
   * Retrieve a TCP record by its ID.
   * @param {Object} params - Parameters object containing customerId and id.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object|null>} The TCP record or null if not found.
   */
  getById,

  /**
   * Create a new TCP record.
   * @param {Object} params - Parameters object containing customerId and TCP data.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object>} The created TCP record.
   */
  create,

  /**
   * Update a TCP record by ID.
   * @param {Object} params - Parameters object containing customerId.
   * @param {Object} options - Query options.
   * @returns {Promise<Object|null>} The updated TCP record or null if not found.
   */
  update,

  /**
   * Delete a TCP record by ID.
   * @param {Object} params - Parameters object containing customerId.
   * @param {Object} options - Query options.
   * @returns {Promise<void>}
   */
  delete: _delete,

  /**
   * Check if there are any TCP records missing the isSb flag.
   * @param {Object} params - Parameters object containing customerId.
   * @param {Object} [options] - Query options.
   * @returns {Promise<boolean>} True if any records are missing the isSb flag.
   */
  hasMissingIsSbFlag,

  /**
   * Finalise PTRS submission.
   * @param {Object} params - Parameters object containing customerId.
   * @param {Object} [options] - Options.
   * @returns {Promise<Object>} Result of finalisation.
   */
  finalisePtrs,

  /**
   * Generate a CSV summary of TCP records.
   * @param {Object} params - Parameters object containing customerId.
   * @param {Object} [options] - Query options.
   * @returns {Promise<string>} CSV string.
   */
  generateSummaryCsv,

  /**
   * Partially update a TCP record by ID.
   * @param {Object} params - Parameters object containing customerId and id.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object|null>} The updated TCP record or null if not found.
   */
  partialUpdate,

  /**
   * Patch a TCP record by ID.
   * @param {Object} params - Parameters object containing customerId, id, and update data.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object|null>} The patched TCP record or null if not found.
   */
  patchRecord,
  bulkPatchUpdate,
  bulkDelete,
  bulkDeleteErrors,

  /**
   * Get current value of a specific field for a TCP record.
   * @param {Object} params - Parameters object containing tcpId and field_name.
   * @param {Object} [options] - Query options.
   * @returns {Promise<any>} Current field value or null if not found.
   */
  getCurrentFieldValue,

  /**
   * Save transformed TCP records.
   * @param {Object} params - Parameters object containing customerId, transformedRecords, ptrsId, createdBy, source.
   * @param {Object} [options] - Options.
   * @returns {Promise<Array>} Inserted TCP records.
   */
  saveTransformedDataToTcp,

  /**
   * Save TCP error records.
   * @param {Object} params - Parameters object containing customerId, errorRecords, ptrsId, createdBy, source.
   * @param {Object} [options] - Options.
   * @returns {Promise<boolean>} True if saved successfully.
   */
  saveErrorsToTcpError,

  /**
   * Get TCP error records by PTRS ID.
   * @param {Object} params - Parameters object containing ptrsId.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Array>} List of TCP error records.
   */
  getErrorsByPtrsId,
  resolveErrors,
};

async function getAll(customerId, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const baseWhere = { excludedTcp: false };
    const mergedWhere =
      options && options.where ? { ...baseWhere, ...options.where } : baseWhere;
    const { where: _omitWhere, ...rest } = options || {};
    const rows = await db.Tcp.findAll({
      where: mergedWhere,
      ...rest,
      transaction: t,
    });
    await t.commit();
    return rows.map((row) => row.get({ plain: true }));
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getByPtrsId(params, options = {}) {
  const { customerId, ptrsId } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const baseWhere = { ptrsId, excludedTcp: false };
    const mergedWhere =
      options && options.where ? { ...baseWhere, ...options.where } : baseWhere;
    const { where: _omitWhere, ...rest } = options || {};
    const rows = await db.Tcp.findAll({
      where: mergedWhere,
      ...rest,
      transaction: t,
    });
    await t.commit();
    return rows.map((row) => row.get({ plain: true }));
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function sbiUpdate(params, options = {}) {
  const { ptrsId, payeeEntityAbn, customerId, updatedBy } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Tcp.update(
      { isSb: false, ...(updatedBy && { updatedBy }) },
      {
        where: {
          ptrsId,
          payeeEntityAbn,
        },
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    // No return
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getById(params, options = {}) {
  const { customerId, id } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const baseWhere = { id };
    const mergedWhere =
      options && options.where ? { ...baseWhere, ...options.where } : baseWhere;
    const row = await db.Tcp.findOne({
      where: { ...mergedWhere, excludedTcp: false },
      transaction: t,
      ...options,
    });
    await t.commit();
    return row ? row.get({ plain: true }) : null;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function create(params, options = {}) {
  const { customerId, createdBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const record = await db.Tcp.create(
      { ...rest, ...(createdBy && { createdBy }) },
      { transaction: t, ...options }
    );
    await t.commit();
    return record.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function update(params, options = {}) {
  const { customerId, id, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Tcp.update(
      { ...rest, ...(updatedBy && { updatedBy }) },
      { where: { id }, transaction: t, ...options }
    );
    const record = await db.Tcp.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
    return record ? record.get({ plain: true }) : null;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function _delete(params, options = {}) {
  const { customerId, id } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Tcp.destroy({ where: { id }, transaction: t, ...options });
    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function bulkDelete(params = {}, options = {}) {
  const { customerId, ids, ptrsId } = params;
  if (!Array.isArray(ids) || ids.length === 0) {
    throw new Error("ids must be a non-empty array");
  }
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = { id: ids };
    if (ptrsId) where.ptrsId = ptrsId;

    const affected = await db.Tcp.destroy({
      where,
      transaction: t,
      ...(options || {}),
    });

    await t.commit();
    return affected; // number of rows soft-deleted
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function bulkDeleteErrors(params = {}, options = {}) {
  const { customerId, ids, ptrsId } = params;
  if (!Array.isArray(ids) || ids.length === 0) {
    throw new Error("ids must be a non-empty array");
  }
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = { id: ids };
    if (ptrsId) where.ptrsId = ptrsId;

    const affected = await db.TcpError.destroy({
      where,
      transaction: t,
      ...(options || {}),
    });

    await t.commit();
    return affected; // number of error rows deleted (soft if model is paranoid)
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function hasMissingIsSbFlag(params = {}, options = {}) {
  const { customerId } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const count = await db.Tcp.count({
      where: {
        isTcp: true,
        excludedTcp: false,
        isSb: null,
      },
      transaction: t,
      ...options,
    });
    await t.commit();
    return count > 0;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function finalisePtrs(params = {}, options = {}) {
  // This is delegated, but we keep signature for consistency.
  return await ptrsService.finaliseSubmission(params, options);
}

async function generateSummaryCsv(params = {}, options = {}) {
  const { customerId } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.Tcp.findAll({
      attributes: [
        "payeeEntityName",
        "payeeEntityAbn",
        "paymentAmount",
        "paymentDate",
        "invoiceIssueDate",
        "isSb",
        "paymentTime",
      ],
      where: {
        isTcp: true,
        excludedTcp: false,
      },
      raw: true,
      transaction: t,
      ...options,
    });
    await t.commit();
    const header =
      "Payee Name,ABN,Amount,Payment Date,Invoice Date,Is Small Business,Payment Time";
    const csv = [
      header,
      ...rows.map(
        (r) =>
          `"${r.payeeEntityName}","${r.payeeEntityAbn}",${r.paymentAmount},"${r.paymentDate}","${r.invoiceIssueDate}",${r.isSb},${r.paymentTime}`
      ),
    ].join("\n");
    return csv;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function partialUpdate(params, options = {}) {
  const { customerId, id, updates, updatedBy } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Tcp.update(
      { ...updates, ...(updatedBy && { updatedBy }) },
      { where: { id }, transaction: t, ...options }
    );
    const record = await db.Tcp.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
    return record ? record.get({ plain: true }) : null;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function patchRecord(params, options = {}) {
  const { customerId, id, update, updatedBy } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Tcp.update(
      { ...update, ...(updatedBy && { updatedBy }) },
      { where: { id }, transaction: t }
    );
    const record = await db.Tcp.findOne({ where: { id }, transaction: t });
    await t.commit();
    return record ? record.get({ plain: true }) : null;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getCurrentFieldValue(params, options = {}) {
  const { tcpId, field_name, customerId } = params;
  const allowedFields = [
    "payeeEntityName",
    "payeeEntityAbn",
    "paymentAmount",
    "paymentDate",
    "invoiceIssueDate",
    "isSb",
    "paymentTime",
    "isTcp",
    "excludedTcp",
  ];
  if (!allowedFields.includes(field_name)) {
    throw new Error("Requested field_name is not valid");
  }
  const t = customerId
    ? await beginTransactionWithCustomerContext(customerId)
    : null;
  try {
    const row = await db.Tcp.findOne({
      attributes: [field_name],
      where: { id: tcpId },
      raw: true,
      ...(t ? { transaction: t } : {}),
      ...options,
    });
    if (t) await t.commit();
    return row ? row[field_name] : null;
  } catch (error) {
    if (t && !t.finished) await t.rollback();
    throw error;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}

async function saveTransformedDataToTcp(params, options = {}) {
  const {
    customerId,
    transformedRecords,
    ptrsId,
    createdBy,
    source = "xero",
  } = params;
  if (!Array.isArray(transformedRecords)) {
    throw new Error("Transformed TCP records must be an array");
  }

  transformedRecords.forEach((record) => {
    // Coerce paymentAmount if provided as a string
    if (typeof record.paymentAmount === "string") {
      const cleanedPay = record.paymentAmount.replace(/[^0-9.-]+/g, "").trim();
      record.paymentAmount =
        cleanedPay === "" ? undefined : parseFloat(cleanedPay);
    }

    // NEW: Coerce invoiceAmount like paymentAmount; drop if empty/non-numeric
    if (record.invoiceAmount != null) {
      if (typeof record.invoiceAmount === "string") {
        const cleanedInv = record.invoiceAmount
          .replace(/[^0-9.-]+/g, "")
          .trim();
        if (cleanedInv === "") {
          delete record.invoiceAmount; // treat as not provided
        } else {
          const invNum = parseFloat(cleanedInv);
          if (Number.isNaN(invNum)) {
            delete record.invoiceAmount;
          } else {
            record.invoiceAmount = invNum;
          }
        }
      } else if (typeof record.invoiceAmount !== "number") {
        // Non-string, non-number -> remove to avoid Joi number type error
        delete record.invoiceAmount;
      }
    }

    // Date coercions using normaliser
    record.supplyDate = parseDateLike(record.supplyDate);
    record.paymentDate = parseDateLike(record.paymentDate);
    record.invoiceIssueDate = parseDateLike(record.invoiceIssueDate);
    record.invoiceReceiptDate = parseDateLike(record.invoiceReceiptDate);
    record.invoiceDueDate = parseDateLike(record.invoiceDueDate);
    record.noticeForPaymentIssueDate = parseDateLike(
      record.noticeForPaymentIssueDate
    );
  });

  for (let i = 0; i < transformedRecords.length; i++) {
    transformedRecords[i].createdBy = createdBy;
    transformedRecords[i].ptrsId = ptrsId;
    transformedRecords[i].customerId = customerId;
    transformedRecords[i].source = source;
    const { error } = tcpBulkImportSchema.validate(transformedRecords[i]);
    if (error) {
      throw new Error(
        `Validation error in record at index ${i}: ${error.message}`
      );
    }
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const { validate = true } = options || {};
    await db.Tcp.bulkCreate(transformedRecords, {
      validate,
      transaction: t,
    });
    const insertedRecords = await db.Tcp.findAll({
      where: { ptrsId },
      transaction: t,
    });
    await t.commit();
    return insertedRecords.map((r) => r.get({ plain: true }));
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function saveErrorsToTcpError(params, options = {}) {
  const { customerId, errorRecords, ptrsId, createdBy, source } = params;
  if (!Array.isArray(errorRecords)) {
    throw new Error("TCP error records must be an array");
  }

  const attrs =
    db.TcpError && db.TcpError.rawAttributes ? db.TcpError.rawAttributes : {};
  const allowedAttrs = Object.keys(attrs);

  const numKeys = new Set(
    Object.entries(attrs)
      .filter(
        ([, a]) =>
          a &&
          a.type &&
          typeof a.type.key === "string" &&
          ["DECIMAL", "INTEGER", "FLOAT", "BIGINT", "REAL", "DOUBLE"].includes(
            a.type.key.toUpperCase()
          )
      )
      .map(([k]) => k)
  );
  const dateKeys = new Set(
    Object.entries(attrs)
      .filter(
        ([, a]) =>
          a &&
          a.type &&
          typeof a.type.key === "string" &&
          ["DATE", "DATEONLY"].includes(a.type.key.toUpperCase())
      )
      .map(([k]) => k)
  );
  const boolKeys = new Set(
    Object.entries(attrs)
      .filter(
        ([, a]) =>
          a &&
          a.type &&
          typeof a.type.key === "string" &&
          a.type.key.toUpperCase() === "BOOLEAN"
      )
      .map(([k]) => k)
  );

  function coerceNumber(v) {
    if (v == null) return null;
    if (typeof v === "number") return Number.isNaN(v) ? null : v;
    const s = String(v).trim();
    if (s === "") return null;
    const cleaned = s.replace(/[^0-9.-]+/g, "");
    if (
      cleaned === "" ||
      cleaned === "-" ||
      cleaned === "." ||
      cleaned === "-."
    )
      return null;
    const n = parseFloat(cleaned);
    return Number.isNaN(n) ? null : n;
  }
  function coerceDate(v) {
    return parseDateLike(v);
  }
  function coerceBool(v) {
    if (v == null || v === "") return null;
    if (typeof v === "boolean") return v;
    const s = String(v).trim().toLowerCase();
    if (["1", "true", "t", "y", "yes"].includes(s)) return true;
    if (["0", "false", "f", "n", "no"].includes(s)) return false;
    return null;
  }

  const sanitized = errorRecords.map((rec) => {
    const base = { ...rec, createdBy, ptrsId, customerId, source };

    const out = {};
    for (const k of allowedAttrs) {
      if (base[k] === undefined) continue;
      let val = base[k];
      if (numKeys.has(k)) val = coerceNumber(val);
      else if (dateKeys.has(k)) val = coerceDate(val);
      else if (boolKeys.has(k)) val = coerceBool(val);
      else if (k === "issues") {
        val = Array.isArray(val) ? JSON.stringify(val) : String(val);
      }
      out[k] = val;
    }

    // Ensure required metadata fields are included if present in model
    if (allowedAttrs.includes("createdBy")) out.createdBy = base.createdBy;
    if (allowedAttrs.includes("ptrsId")) out.ptrsId = base.ptrsId;
    if (allowedAttrs.includes("customerId")) out.customerId = base.customerId;
    if (allowedAttrs.includes("source")) out.source = base.source;

    return out;
  });

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const { validate = true } = options || {};
    await db.TcpError.bulkCreate(sanitized, { validate, transaction: t });
    await t.commit();
    return true;
  } catch (error) {
    if (!t.finished) await t.rollback();
    if (error && Array.isArray(error.errors) && error.errors.length) {
      const messages = error.errors.map((e) => e && (e.message || String(e)));
      throw new Error(`TcpError bulkCreate failed: ${messages.join("; ")}`);
    }
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getErrorsByPtrsId(params, options = {}) {
  const {
    ptrsId,
    customerId,
    start,
    end,
    page: pageParam,
    pageSize: pageSizeParam,
    order = "ASC",
  } = params || {};

  const page = Math.max(1, Number(pageParam) || 1);
  const pageSizeCap = 500;
  const pageSize = Math.min(
    pageSizeCap,
    Math.max(1, Number(pageSizeParam) || 100)
  );
  const offset = (page - 1) * pageSize;

  const t = customerId
    ? await beginTransactionWithCustomerContext(customerId)
    : null;

  try {
    const where = { ptrsId };

    // optional createdAt range: [start, end)
    if (start || end) {
      where.createdAt = {};
      if (start) where.createdAt[Op.gte] = start;
      if (end) where.createdAt[Op.lt] = end;
    }

    const { rows, count } = await db.TcpError.findAndCountAll({
      where,
      limit: pageSize,
      offset,
      order: [
        ["createdAt", String(order).toUpperCase() === "DESC" ? "DESC" : "ASC"],
      ],
      ...(t ? { transaction: t } : {}),
      ...options,
    });

    if (t) await t.commit();

    const data = rows.map((r) => r.get({ plain: true }));
    const total =
      typeof count === "number"
        ? count
        : Array.isArray(count)
          ? count.length
          : 0;

    return { data, total, page, pageSize };
  } catch (error) {
    if (t && !t.finished) await t.rollback();
    throw error;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}

// Atomically promote error rows from TcpError to Tcp and delete the error rows
async function resolveErrors(params, options = {}) {
  // console.log("params: ", params);
  const { customerId, userId, records } = params;
  if (!Array.isArray(records)) {
    throw new Error("records must be an array");
  }
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // Prepare insert payloads for Tcp and collect ids to delete from TcpError
    const toInsert = [];
    const errorIds = [];
    for (const rec of records) {
      const clean = { ...rec };
      // Remove TcpError / DB-managed fields early
      delete clean.id;
      delete clean.createdAt;
      delete clean.updatedAt;
      delete clean.errorReason;
      delete clean.issues;

      // Coerce numerics similar to saveTransformedDataToTcp
      if (typeof clean.paymentAmount === "string") {
        const cleaned = clean.paymentAmount.replace(/[^0-9.-]+/g, "").trim();
        clean.paymentAmount = cleaned === "" ? undefined : parseFloat(cleaned);
      }
      if (
        clean.invoiceAmount != null &&
        typeof clean.invoiceAmount === "string"
      ) {
        const cleanedInv = clean.invoiceAmount.replace(/[^0-9.-]+/g, "").trim();
        if (cleanedInv === "") delete clean.invoiceAmount;
        else {
          const invNum = parseFloat(cleanedInv);
          if (Number.isNaN(invNum)) delete clean.invoiceAmount;
          else clean.invoiceAmount = invNum;
        }
      }
      // Coerce dates using normaliser
      clean.supplyDate = parseDateLike(clean.supplyDate);
      clean.paymentDate = parseDateLike(clean.paymentDate);
      clean.invoiceIssueDate = parseDateLike(clean.invoiceIssueDate);
      clean.invoiceReceiptDate = parseDateLike(clean.invoiceReceiptDate);
      clean.invoiceDueDate = parseDateLike(clean.invoiceDueDate);
      clean.noticeForPaymentIssueDate = parseDateLike(
        clean.noticeForPaymentIssueDate
      );

      // Coerce isReconciled to boolean if present (string/number → boolean, drop invalid)
      if (Object.prototype.hasOwnProperty.call(clean, "isReconciled")) {
        const val = clean.isReconciled;
        if (typeof val === "boolean") {
          // ok as-is
        } else if (typeof val === "number") {
          clean.isReconciled =
            val === 1 ? true : val === 0 ? false : Boolean(val);
        } else if (typeof val === "string") {
          const lower = val.trim().toLowerCase();
          if (["1", "true", "t", "y", "yes"].includes(lower)) {
            clean.isReconciled = true;
          } else if (["0", "false", "f", "n", "no"].includes(lower)) {
            clean.isReconciled = false;
          } else if (
            lower === "" ||
            lower === "null" ||
            lower === "undefined"
          ) {
            delete clean.isReconciled; // treat empty as absent
          } else {
            delete clean.isReconciled; // drop invalid to satisfy Joi.boolean()
          }
        } else if (val == null) {
          delete clean.isReconciled; // null/undefined → absent
        } else {
          delete clean.isReconciled; // any other type → absent
        }
      }

      // Ensure required metadata
      clean.customerId = customerId;
      if (userId) {
        clean.createdBy = userId;
        clean.updatedBy = userId;
      }

      // Whitelist of fields allowed for Tcp insert (must align with validator/model)
      const allowedInsertKeys = [
        "payerEntityName",
        "payerEntityAbn",
        "payerEntityAcnArbn",
        "payeeEntityName",
        "payeeEntityAbn",
        "payeeEntityAcnArbn",
        "paymentAmount",
        "description",
        "transactionType",
        "isReconciled",
        "supplyDate",
        "paymentDate",
        "contractPoReferenceNumber",
        "contractPoPaymentTerms",
        "noticeForPaymentIssueDate",
        "noticeForPaymentTerms",
        "invoiceReferenceNumber",
        "invoiceIssueDate",
        "invoiceReceiptDate",
        "invoiceAmount",
        "invoicePaymentTerms",
        "invoiceDueDate",
        "accountCode",
        "isTcp",
        "tcpExclusionComment",
        "peppolEnabled",
        "rcti",
        "creditCardPayment",
        "creditCardNumber",
        "partialPayment",
        "paymentTerm",
        "excludedTcp",
        "explanatoryComments1",
        "isSb",
        "paymentTime",
        "explanatoryComments2",
        // metadata
        "source",
        "createdBy",
        "updatedBy",
        "ptrsId",
        "customerId",
      ];

      // Filter to allowed fields only
      const filtered = Object.fromEntries(
        Object.entries(clean).filter(([k]) => allowedInsertKeys.includes(k))
      );

      // Validate as a normal Tcp import row (against filtered payload)
      const { error } = tcpBulkImportSchema.validate(filtered);
      if (error) {
        throw new Error(
          `Validation error promoting error row ${rec.id || ""}: ${error.message}`
        );
      }

      toInsert.push(filtered);
      if (rec.id) errorIds.push(rec.id);
    }

    // Insert into Tcp
    await db.Tcp.bulkCreate(toInsert, {
      validate: true,
      transaction: t,
      ...(options || {}),
    });

    // Delete from TcpError if we have ids
    if (errorIds.length) {
      await db.TcpError.destroy({ where: { id: errorIds }, transaction: t });
    }

    await t.commit();
    return toInsert.length;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

// Bulk patch update multiple TCP records
async function bulkPatchUpdate(params, options = {}) {
  const { customerId, userId, records } = params || {};
  if (!Array.isArray(records)) {
    throw new Error("records must be an array");
  }
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const normalised = records.map((r) => {
      if (!r || typeof r !== "object")
        throw new Error("Invalid record payload");
      const { id } = r;
      if (!id) throw new Error("Each record must include an id");
      const update =
        r.fields && typeof r.fields === "object"
          ? { ...r.fields }
          : (() => {
              const { id: _id, fields: _fields, ...rest } = r; // spread form
              return { ...rest };
            })();
      return { id, update };
    });

    // Allowed update keys (mirror of insert list minus immutable/meta)
    const allowedUpdateKeys = [
      "payerEntityName",
      "payerEntityAbn",
      "payerEntityAcnArbn",
      "payeeEntityName",
      "payeeEntityAbn",
      "payeeEntityAcnArbn",
      "paymentAmount",
      "description",
      "transactionType",
      "isReconciled",
      "supplyDate",
      "paymentDate",
      "contractPoReferenceNumber",
      "contractPoPaymentTerms",
      "noticeForPaymentIssueDate",
      "noticeForPaymentTerms",
      "invoiceReferenceNumber",
      "invoiceIssueDate",
      "invoiceReceiptDate",
      "invoiceAmount",
      "invoicePaymentTerms",
      "invoiceDueDate",
      "accountCode",
      "isTcp",
      "tcpExclusionComment",
      "peppolEnabled",
      "rcti",
      "creditCardPayment",
      "creditCardNumber",
      "partialPayment",
      "paymentTerm",
      "excludedTcp",
      "explanatoryComments1",
      "isSb",
      "paymentTime",
      "explanatoryComments2",
      // metadata we allow to modify
      "updatedBy",
    ];

    const updatedIds = [];
    for (const { id, update } of normalised) {
      if (!update || typeof update !== "object") continue;
      // Strip forbidden keys
      const cleaned = { ...update };
      delete cleaned.id;
      delete cleaned.customerId;
      delete cleaned.ptrsId;
      delete cleaned.createdAt;
      delete cleaned.updatedAt;
      delete cleaned.createdBy;
      if (userId) cleaned.updatedBy = userId;

      // Filter to allowed keys only
      const filtered = Object.fromEntries(
        Object.entries(cleaned).filter(([k]) => allowedUpdateKeys.includes(k))
      );

      if (Object.keys(filtered).length === 0) {
        continue; // nothing to update
      }

      await db.Tcp.update(filtered, {
        where: { id },
        transaction: t,
        ...(options || {}),
      });
      updatedIds.push(id);
    }

    const updatedRows = updatedIds.length
      ? await db.Tcp.findAll({ where: { id: updatedIds }, transaction: t })
      : [];

    await t.commit();
    return updatedRows.map((r) => r.get({ plain: true }));
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}
