const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
const db = require("../db/database");

module.exports = {
  getAll,
  getById,
  create,
  update,
  patch,
  delete: _delete,
  getAllByPtrsId,
  finaliseSubmission,
  saveUploadMetadata,
};

async function getAll({ customerId, includeDeleted = false, ...options } = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const whereClause = includeDeleted
      ? {}
      : { status: { [db.Sequelize.Op.ne]: "Deleted" } };

    const rows = await db.Ptrs.findAll({
      where: whereClause,
      order: [["createdAt", "DESC"]],
      ...options,
      transaction: t,
    });

    await t.commit();

    return rows.map((row) => row.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getAllByPtrsId({ ptrsId, ...options } = {}) {
  const t = await beginTransactionWithCustomerContext(options.customerId);
  try {
    const rows = await db.Ptrs.findAll({
      where: { ptrsId },
      ...options,
      transaction: t,
    });

    await t.commit();

    return rows.map((row) => row.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function create({ data, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const result = await db.Ptrs.create(
      {
        ...data,
        customerId,
        createdBy: data.createdBy,
        updatedBy: data.updatedBy,
      },
      {
        ...options,
        transaction: t,
      }
    );

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function update({ id, data, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Ptrs.update(
      { ...data, updatedBy: userId },
      {
        where: { id },
        ...options,
        transaction: t,
      }
    );
    const result = await db.Ptrs.findOne({
      where: { id },
      ...options,
      transaction: t,
    });

    if (!result) {
      throw { status: 404, message: "Ptrs not found" };
    }

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function patch({
  id,
  data,
  customerId,
  userId,
  transaction,
  ...options
}) {
  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  try {
    const [count, [updatedPtrs]] = await db.Ptrs.update(
      { ...data, updatedBy: userId },
      {
        where: { id },
        returning: true,
        ...options,
        transaction: t,
      }
    );

    if (count === 0) {
      throw {
        status: 404,
        message: "Ptrs not found or update blocked by RLS.",
      };
    }
    if (!transaction) await t.commit();
    return updatedPtrs.get({ plain: true });
  } catch (error) {
    if (!transaction) await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!transaction && !t.finished) await t.rollback();
  }
}

async function _delete({ id, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count] = await db.Ptrs.update(
      { status: "Deleted", updatedBy: userId },
      {
        ...options,
        where: { id },
        transaction: t,
      }
    );

    if (count === 0) {
      throw {
        status: 404,
        message: "Ptrs not found or update blocked by RLS.",
      };
    }

    await t.commit();
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getById({ id, customerId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const report = await db.Ptrs.findOne({
      where: { id },
      transaction: t,
    });
    if (!report) {
      throw { status: 404, message: "Ptrs not found" };
    }
    await t.commit();
    return report.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function finaliseSubmission() {
  const viewName = `customer_${db.sequelize.config.database}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT COUNT(*) AS count FROM "${viewName}" WHERE isTcp = true AND excludedTcp = false AND isSb IS NULL`
  );
  if (rows[0].count > 0) {
    throw { status: 400, message: "Some records are missing isSb flags" };
  }

  const [ptrsIds] = await db.sequelize.query(
    `SELECT DISTINCT ptrsId FROM "${viewName}" WHERE isTcp = true AND excludedTcp = false`
  );

  const now = new Date();
  const updatePayload = {
    status: "Submitted",
    submittedDate: now,
  };

  for (const { ptrsId } of ptrsIds) {
    await db.Ptrs.update(updatePayload, {
      where: { id: ptrsId },
    });
  }

  return { success: true, message: "Ptrs(s) marked as Submitted" };
}

/**
 * Save upload metadata to the reportUpload table.
 * @param {Object} metadata - The metadata to save.
 * @param {Object} options - Optional Sequelize options (e.g. transaction).
 * @returns {Promise<Object>} The created PtrsUpload instance.
 */
async function saveUploadMetadata(metadata, options = {}) {
  const externalTx = options.transaction || null;
  const customerId = metadata.customerId;

  // Start our own transaction if caller didn't provide one
  const t =
    externalTx || (await beginTransactionWithCustomerContext(customerId));
  try {
    if (!t.finished) {
      await db.sequelize.query(
        `SET LOCAL app.current_customer_id = '${customerId}'`,
        { transaction: t }
      );
    }

    const result = await db.PtrsUpload.create(metadata, { transaction: t });

    if (!externalTx) await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    if (!externalTx && !t.finished) await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!externalTx && !t.finished) await t.rollback();
  }
}
