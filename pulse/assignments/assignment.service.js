const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const db = require("@/db/database");

module.exports = {
  getAll,
  getById,
  create,
  update,
  patch,
  delete: _delete,
  listByBudgetItem,
};

async function getAll({
  customerId,
  budgetItemId,
  trackableId,
  ...options
} = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // If we are filtering by trackable, resolve the set of budgetItemIds
    // that belong to any budget for that trackable, then fetch assignments
    // for those budget items. This avoids assuming a trackableId column on Assignment.
    if (trackableId && !budgetItemId) {
      // 1) budgets for this trackable
      const budgets = await db.Budget.findAll({
        attributes: ["id"],
        where: { trackableId },
        transaction: t,
      });
      const budgetIds = budgets.map((b) => b.get("id"));
      if (budgetIds.length === 0) {
        await t.commit();
        return [];
      }

      // 2) budget items for those budgets
      const items = await db.BudgetItem.findAll({
        attributes: ["id"],
        where: { budgetId: budgetIds },
        transaction: t,
      });
      const budgetItemIds = items.map((i) => i.get("id"));
      if (budgetItemIds.length === 0) {
        await t.commit();
        return [];
      }

      const rows = await db.Assignment.findAll({
        where: { budgetItemId: budgetItemIds },
        order: [["createdAt", "DESC"]],
        ...options,
        transaction: t,
      });
      await t.commit();
      return rows.map((r) => r.get({ plain: true }));
    }

    // Default path: direct filters
    const where = {};
    if (budgetItemId) where.budgetItemId = budgetItemId;

    const rows = await db.Assignment.findAll({
      where,
      order: [["createdAt", "DESC"]],
      ...options,
      transaction: t,
    });
    await t.commit();
    return rows.map((r) => r.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function listByBudgetItem({ budgetItemId, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.Assignment.findAll({
      where: { budgetItemId },
      ...options,
      transaction: t,
    });
    await t.commit();
    return rows.map((r) => r.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getById({ id, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.Assignment.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!row) throw { status: 404, message: "Assignment not found" };
    await t.commit();
    return row.get({ plain: true });
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
    const result = await db.Assignment.create(
      {
        ...data,
        customerId,
        createdBy: data.createdBy,
        updatedBy: data.updatedBy,
      },
      { ...options, transaction: t }
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
    await db.Assignment.update(
      { ...data, updatedBy: userId },
      { where: { id }, ...options, transaction: t }
    );
    const result = await db.Assignment.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Assignment not found" };
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
    const [count, [updated]] = await db.Assignment.update(
      { ...data, updatedBy: userId },
      { where: { id }, returning: true, ...options, transaction: t }
    );
    if (count === 0)
      throw {
        status: 404,
        message: "Assignment not found or update blocked by RLS.",
      };
    if (!transaction) await t.commit();
    return updated.get({ plain: true });
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
    const count = await db.Assignment.destroy({
      where: { id },
      ...options,
      transaction: t,
    });
    if (count === 0)
      throw {
        status: 404,
        message: "Assignment not found or delete blocked by RLS.",
      };
    await t.commit();
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}
