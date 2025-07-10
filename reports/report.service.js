const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");

module.exports = {
  getAll,
  getById,
  create,
  update,
  patch,
  delete: _delete,
  getAllByReportId,
  finaliseSubmission,
  saveUploadMetadata,
};

async function getAll(options = {}) {
  const t = await beginTransactionWithClientContext(options.clientId);
  try {
    const whereClause = options.includeDeleted
      ? {}
      : { reportStatus: { [db.Sequelize.Op.ne]: "Deleted" } };

    const rows = await db.Report.findAll({
      where: whereClause,
      ...options,
      transaction: t,
    });

    return rows;
  } catch (error) {
    throw error;
  }
}

async function getAllByReportId(reportId, options = {}) {
  try {
    const rows = await db.Report.findAll({
      where: { reportId },
      ...options,
      transaction: options.transaction,
    });
    return rows;
  } catch (error) {
    throw error;
  }
}

async function create(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.Report.create(params, {
      ...options,
      transaction: t,
    });

    await t.commit();

    return result?.get?.({ plain: true }) || result;
  } catch (error) {
    await t.rollback();
    throw error;
  }
}

async function update(id, params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    await db.Report.update(params, {
      where: { id },
      ...options,
      transaction: t,
    });
    const result = await db.Report.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    await t.commit();
    return result?.get?.({ plain: true }) || result;
  } catch (error) {
    await t.rollback();
    throw error;
  }
}

async function patch(id, params, options = {}) {
  const t =
    options.transaction ||
    (await beginTransactionWithClientContext(params.clientId));
  try {
    const [count, [updatedReport]] = await db.Report.update(params, {
      where: { id },
      returning: true,
      ...options,
      transaction: t,
    });

    if (count === 0) {
      throw new Error("Report not found or update blocked by RLS.");
    }
    if (!options.transaction) await t.commit();
    return updatedReport?.get?.({ plain: true }) || updatedReport;
  } catch (error) {
    if (!options.transaction) await t.rollback();
    throw error;
  }
}

async function _delete(id, clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count] = await db.Report.update(
      { reportStatus: "Deleted" },
      {
        ...options,
        where: { id },
        transaction: t,
      }
    );

    if (count === 0) {
      throw new Error("Report not found or update blocked by RLS.");
    }

    await t.commit();
  } catch (error) {
    await t.rollback();
    throw error;
  }
}

async function getById(id, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const report = await db.Report.findOne({
      where: { id },
      transaction: t,
    });
    if (!report) {
      throw { status: 404, message: "Report not found" };
    }
    return report.get({ plain: true });
  } catch (error) {
    throw error;
  }
}

async function finaliseSubmission() {
  const viewName = `client_${db.sequelize.config.database}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT COUNT(*) AS count FROM "${viewName}" WHERE isTcp = true AND excludedTcp = false AND isSb IS NULL`
  );
  if (rows[0].count > 0) {
    throw { status: 400, message: "Some records are missing isSb flags" };
  }

  const [reportIds] = await db.sequelize.query(
    `SELECT DISTINCT reportId FROM "${viewName}" WHERE isTcp = true AND excludedTcp = false`
  );

  const now = new Date();
  const updatePayload = {
    reportStatus: "Submitted",
    submittedDate: now,
  };

  for (const { reportId } of reportIds) {
    await db.Report.update(updatePayload, {
      where: { id: reportId },
    });
  }

  return { success: true, message: "Report(s) marked as Submitted" };
}

/**
 * Save upload metadata to the reportUpload table.
 * @param {Object} metadata - The metadata to save.
 * @param {Object} options - Optional Sequelize options (e.g. transaction).
 * @returns {Promise<Object>} The created ReportUpload instance.
 */
async function saveUploadMetadata(metadata, options = {}) {
  const transaction = options.transaction;
  const clientId = metadata.clientId;

  if (!transaction) {
    throw new Error("Transaction is required for saveUploadMetadata");
  }

  await db.sequelize.query(`SET LOCAL app.current_client_id = '${clientId}'`, {
    transaction,
  });

  const result = await db.ReportUpload.create(metadata, { transaction });

  return result;
}
