const db = require("../db/database");
const ptrsService = require("../ptrs/ptrs.service");
const { tcpBulkImportSchema } = require("./tcp.validator");
const { sequelize } = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

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
   * @param {Object} params - Parameters object containing clientId and ptrsId.
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
   * @param {Object} params - Parameters object containing clientId and id.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object|null>} The TCP record or null if not found.
   */
  getById,

  /**
   * Create a new TCP record.
   * @param {Object} params - Parameters object containing clientId and TCP data.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object>} The created TCP record.
   */
  create,

  /**
   * Update a TCP record by ID.
   * @param {Object} params - Parameters object containing clientId.
   * @param {Object} options - Query options.
   * @returns {Promise<Object|null>} The updated TCP record or null if not found.
   */
  update,

  /**
   * Delete a TCP record by ID.
   * @param {Object} params - Parameters object containing clientId.
   * @param {Object} options - Query options.
   * @returns {Promise<void>}
   */
  delete: _delete,

  /**
   * Check if there are any TCP records missing the isSb flag.
   * @param {Object} params - Parameters object containing clientId.
   * @param {Object} [options] - Query options.
   * @returns {Promise<boolean>} True if any records are missing the isSb flag.
   */
  hasMissingIsSbFlag,

  /**
   * Finalise PTRS submission.
   * @param {Object} params - Parameters object containing clientId.
   * @param {Object} [options] - Options.
   * @returns {Promise<Object>} Result of finalisation.
   */
  finalisePtrs,

  /**
   * Generate a CSV summary of TCP records.
   * @param {Object} params - Parameters object containing clientId.
   * @param {Object} [options] - Query options.
   * @returns {Promise<string>} CSV string.
   */
  generateSummaryCsv,

  /**
   * Partially update a TCP record by ID.
   * @param {Object} params - Parameters object containing clientId and id.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object|null>} The updated TCP record or null if not found.
   */
  partialUpdate,

  /**
   * Patch a TCP record by ID.
   * @param {Object} params - Parameters object containing clientId, id, and update data.
   * @param {Object} [options] - Query options.
   * @returns {Promise<Object|null>} The patched TCP record or null if not found.
   */
  patchRecord,

  /**
   * Get current value of a specific field for a TCP record.
   * @param {Object} params - Parameters object containing tcpId and field_name.
   * @param {Object} [options] - Query options.
   * @returns {Promise<any>} Current field value or null if not found.
   */
  getCurrentFieldValue,

  /**
   * Save transformed TCP records.
   * @param {Object} params - Parameters object containing clientId, transformedRecords, ptrsId, createdBy, source.
   * @param {Object} [options] - Options.
   * @returns {Promise<Array>} Inserted TCP records.
   */
  saveTransformedDataToTcp,

  /**
   * Save TCP error records.
   * @param {Object} params - Parameters object containing clientId, errorRecords, ptrsId, createdBy, source.
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
};

async function getAll(clientId, options = {}) {
  console.log("options: ", options);
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const rows = await db.Tcp.findAll({
      ...options,
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
  const { clientId, ptrsId } = params;
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const rows = await db.Tcp.findAll({
      where: { ptrsId },
      transaction: t,
      ...options,
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
  const { ptrsId, payeeEntityAbn, clientId, updatedBy } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId, id } = params;
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const row = await db.Tcp.findByPk(id, { transaction: t, ...options });
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
  const { clientId, createdBy, ...rest } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId, id, updatedBy, ...rest } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId, id } = params;
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.Tcp.destroy({ where: { id }, transaction: t, ...options });
    await t.commit();
    // Do not return anything
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function hasMissingIsSbFlag(params = {}, options = {}) {
  const { clientId } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId, id, updates, updatedBy } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId, id, update, updatedBy } = params;
  const t = await beginTransactionWithClientContext(clientId);
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
  const { tcpId, field_name, clientId } = params;
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
  const t = clientId ? await beginTransactionWithClientContext(clientId) : null;
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
    clientId,
    transformedRecords,
    ptrsId,
    createdBy,
    source = "xero",
  } = params;
  if (!Array.isArray(transformedRecords)) {
    throw new Error("Transformed TCP records must be an array");
  }

  transformedRecords.forEach((record) => {
    if (typeof record.paymentAmount === "string") {
      record.paymentAmount = parseFloat(
        record.paymentAmount.replace(/[^0-9.-]+/g, "")
      );
    }
    if (
      record.invoiceIssueDate &&
      typeof record.invoiceIssueDate === "string"
    ) {
      record.invoiceIssueDate = new Date(record.invoiceIssueDate);
    }
    if (record.invoiceDueDate && typeof record.invoiceDueDate === "string") {
      record.invoiceDueDate = new Date(record.invoiceDueDate);
    }
  });

  for (let i = 0; i < transformedRecords.length; i++) {
    transformedRecords[i].createdBy = createdBy;
    transformedRecords[i].ptrsId = ptrsId;
    transformedRecords[i].clientId = clientId;
    transformedRecords[i].source = source;
    const { error } = tcpBulkImportSchema.validate(transformedRecords[i]);
    if (error) {
      throw new Error(
        `Validation error in record at index ${i}: ${error.message}`
      );
    }
  }

  const t = await beginTransactionWithClientContext(clientId);
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
  const { clientId, errorRecords, ptrsId, createdBy, source } = params;
  if (!Array.isArray(errorRecords)) {
    throw new Error("TCP error records must be an array");
  }

  for (let i = 0; i < errorRecords.length; i++) {
    errorRecords[i].createdBy = createdBy;
    errorRecords[i].ptrsId = ptrsId;
    errorRecords[i].clientId = clientId;
    errorRecords[i].source = source;
  }

  const t = await beginTransactionWithClientContext(clientId);
  try {
    const { validate = true } = options || {};
    await db.TcpError.bulkCreate(errorRecords, {
      validate,
      transaction: t,
    });
    await t.commit();
    return true;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getErrorsByPtrsId(params, options = {}) {
  const { ptrsId, clientId } = params;
  const t = clientId ? await beginTransactionWithClientContext(clientId) : null;
  try {
    const rows = await db.TcpError.findAll({
      where: { ptrsId },
      ...(t ? { transaction: t } : {}),
      ...options,
    });
    if (t) await t.commit();
    return rows.map((row) => row.get({ plain: true }));
  } catch (error) {
    if (t && !t.finished) await t.rollback();
    throw error;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}
