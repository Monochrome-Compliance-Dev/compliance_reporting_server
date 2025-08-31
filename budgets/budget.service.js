const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
const db = require("../db/database");

// ===== Budget Items (existing) =====
/**
 * List all budget items for a customer (excluding soft-deleted)
 */
async function getAll({ customerId, ...options } = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = { ...(options?.where || {}), deletedAt: null };
    const rows = await db.BudgetItem.findAll({
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

/**
 * List items by engagement (legacy support). Excludes soft-deleted.
 */
async function listByEngagement({ engagementId, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.BudgetItem.findAll({
      where: { engagementId, deletedAt: null },
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

/**
 * List items by budget (preferred). Excludes soft-deleted.
 */
async function listByBudget({ budgetId, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.BudgetItem.findAll({
      where: { budgetId, deletedAt: null },
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

/**
 * Get a single budget item by id (excluding soft-deleted)
 */
async function getById({ id, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.BudgetItem.findOne({
      where: { id, deletedAt: null },
      ...options,
      transaction: t,
    });
    if (!row) throw { status: 404, message: "Budget item not found" };
    await t.commit();
    return row.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

/**
 * Create a budget item
 */
async function create({ data, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const result = await db.BudgetItem.create(
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

/**
 * Update a budget item (PUT)
 */
async function update({ id, data, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.BudgetItem.update(
      { ...data, updatedBy: userId },
      { where: { id, deletedAt: null }, ...options, transaction: t }
    );
    const result = await db.BudgetItem.findOne({
      where: { id, deletedAt: null },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Budget item not found" };
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

/**
 * Patch a budget item (partial update)
 */
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
    const [count, [updated]] = await db.BudgetItem.update(
      { ...data, updatedBy: userId },
      {
        where: { id, deletedAt: null },
        returning: true,
        ...options,
        transaction: t,
      }
    );
    if (count === 0) {
      throw {
        status: 404,
        message: "Budget item not found or update blocked by RLS.",
      };
    }
    if (!transaction) await t.commit();
    return updated.get({ plain: true });
  } catch (error) {
    if (!transaction) await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!transaction && !t.finished) await t.rollback();
  }
}

/**
 * Soft delete a budget item
 */
async function _delete({ id, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count] = await db.BudgetItem.update(
      { deletedAt: new Date(), updatedBy: userId },
      { where: { id, deletedAt: null }, ...options, transaction: t }
    );
    if (count === 0) {
      throw {
        status: 404,
        message: "Budget item not found or delete blocked by RLS.",
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

const budgetItems = {
  getAll,
  getById,
  create,
  update,
  patch,
  delete: _delete,
  listByEngagement,
  listByBudget,
};

// ===== Budget Sections namespace =====
async function sectionsListByBudget({ budgetId, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.BudgetSection.findAll({
      where: { budgetId, deletedAt: null },
      order: options.order || [
        ["order", "ASC"],
        ["createdAt", "ASC"],
      ],
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

async function sectionsCreate({ data, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const result = await db.BudgetSection.create(
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

async function sectionsUpdate({ id, data, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.BudgetSection.update(
      { ...data, updatedBy: userId },
      { where: { id, deletedAt: null }, ...options, transaction: t }
    );
    const result = await db.BudgetSection.findOne({
      where: { id, deletedAt: null },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Budget section not found" };
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function sectionsDelete({ id, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count] = await db.BudgetSection.update(
      { deletedAt: new Date(), updatedBy: userId },
      { where: { id, deletedAt: null }, ...options, transaction: t }
    );
    if (count === 0) {
      throw {
        status: 404,
        message: "Budget section not found or delete blocked by RLS.",
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

const budgetSections = {
  listByBudget: sectionsListByBudget,
  create: sectionsCreate,
  update: sectionsUpdate,
  delete: sectionsDelete,
};

// ===== Budgets (entity) namespace =====
async function budgetsGetAll({ customerId, ...options } = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = { ...(options?.where || {}), deletedAt: null };
    const rows = await db.Budget.findAll({
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

async function budgetsGetById({ id, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.Budget.findOne({
      where: { id, deletedAt: null },
      ...options,
      transaction: t,
    });
    if (!row) throw { status: 404, message: "Budget not found" };
    await t.commit();
    return row.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function budgetsCreate({ data, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const result = await db.Budget.create(
      { ...data, customerId },
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

async function budgetsUpdate({ id, data, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.Budget.update(
      { ...data, updatedBy: userId },
      { where: { id, deletedAt: null }, ...options, transaction: t }
    );
    const result = await db.Budget.findOne({
      where: { id, deletedAt: null },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Budget not found" };
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function budgetsPatch({
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
    const [count, [updated]] = await db.Budget.update(
      { ...data, updatedBy: userId },
      {
        where: { id, deletedAt: null },
        returning: true,
        ...options,
        transaction: t,
      }
    );
    if (count === 0)
      throw {
        status: 404,
        message: "Budget not found or update blocked by RLS.",
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

async function budgetsDelete({ id, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count] = await db.Budget.update(
      { deletedAt: new Date(), updatedBy: userId },
      { where: { id, deletedAt: null }, ...options, transaction: t }
    );
    if (count === 0)
      throw {
        status: 404,
        message: "Budget not found or delete blocked by RLS.",
      };
    await t.commit();
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

// Convenience helper to link a budget to an engagement
async function budgetsLinkToEngagement({
  id,
  engagementId,
  customerId,
  userId,
  transaction,
  ...options
}) {
  // Thin wrapper over budgetsPatch to make intent explicit
  return budgetsPatch({
    id,
    data: { engagementId, customerId, updatedBy: userId },
    customerId,
    userId,
    transaction,
    ...options,
  });
}

const budgets = {
  getAll: budgetsGetAll,
  getById: budgetsGetById,
  create: budgetsCreate,
  update: budgetsUpdate,
  patch: budgetsPatch,
  delete: budgetsDelete,
  linkToEngagement: budgetsLinkToEngagement,
};

module.exports = {
  budgetItems,
  budgets,
  budgetSections,
};
