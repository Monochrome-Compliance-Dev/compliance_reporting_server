const {
  beginTransactionWithCustomerContext,
} = require("../../helpers/setCustomerIdRLS");
const db = require("../../db/database");

// ---- Helpers to enforce immutability on FINAL budgets ----
async function _getBudgetByIdTx({ id, customerId, transaction }) {
  const row = await db.Budget.findOne({
    where: { id, deletedAt: null },
    transaction,
  });
  return row ? row.get({ plain: true }) : null;
}

async function _assertBudgetEditable({ budgetId, customerId, transaction }) {
  const b = await _getBudgetByIdTx({ id: budgetId, customerId, transaction });
  if (!b) throw { status: 404, message: "Budget not found" };
  if (String(b.status).toLowerCase() === "final") {
    throw {
      status: 409,
      message:
        "This budget is Final and cannot be modified. Create a revision to make changes.",
    };
  }
  return b;
}

async function _assertEditableByItemId({ itemId, customerId, transaction }) {
  const item = await db.BudgetItem.findOne({
    where: { id: itemId, deletedAt: null },
    transaction,
  });
  if (!item) throw { status: 404, message: "Budget item not found" };
  const plain = item.get({ plain: true });
  await _assertBudgetEditable({
    budgetId: plain.budgetId,
    customerId,
    transaction,
  });
  return plain;
}

async function _assertEditableBySectionId({
  sectionId,
  customerId,
  transaction,
}) {
  const section = await db.BudgetSection.findOne({
    where: { id: sectionId, deletedAt: null },
    transaction,
  });
  if (!section) throw { status: 404, message: "Budget section not found" };
  const plain = section.get({ plain: true });
  await _assertBudgetEditable({
    budgetId: plain.budgetId,
    customerId,
    transaction,
  });
  return plain;
}

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

async function listByTrackable({ trackableId, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.BudgetItem.findAll({
      where: { trackableId, deletedAt: null },
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
    if (!data?.budgetId) throw { status: 400, message: "budgetId is required" };
    await _assertBudgetEditable({
      budgetId: data.budgetId,
      customerId,
      transaction: t,
    });
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
    await _assertEditableByItemId({ itemId: id, customerId, transaction: t });
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
    await _assertEditableByItemId({ itemId: id, customerId, transaction: t });
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
    await _assertEditableByItemId({ itemId: id, customerId, transaction: t });
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

/**
 * List items by budget with enriched labels for FE selectors.
 * Returns: [{ id, sectionName, budgetItemLabel, trackableName, uiLabel, budgetId }]
 */
async function listEnrichedByBudget({ budgetId, customerId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.sequelize.query(
      `
      SELECT
        i.id                               AS "id",
        i."budgetId"                       AS "budgetId",
        i."sectionName"                    AS "sectionName",
        i."resourceLabel"                  AS "budgetItemLabel",
        t."name"                           AS "trackableName",
        (t."name" || ' — ' || i."sectionName" || ' — ' || i."resourceLabel") AS "uiLabel"
      FROM tbl_pulse_budget_item i
      LEFT JOIN tbl_pulse_budget b
        ON i."budgetId" = b."id"
      LEFT JOIN tbl_pulse_trackable t
        ON b."trackableId" = t."id"
      WHERE i."deletedAt" IS NULL
        AND b."deletedAt" IS NULL
        AND i."budgetId" = :budgetId
      ORDER BY t."name", i."sectionName", i."resourceLabel"
      `,
      {
        replacements: { budgetId },
        type: db.Sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );
    await t.commit();
    return rows;
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
  listByTrackable,
  listByBudget,
  listEnrichedByBudget,
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
    if (!data?.budgetId) throw { status: 400, message: "budgetId is required" };
    await _assertBudgetEditable({
      budgetId: data.budgetId,
      customerId,
      transaction: t,
    });
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
    await _assertEditableBySectionId({
      sectionId: id,
      customerId,
      transaction: t,
    });
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
    await _assertEditableBySectionId({
      sectionId: id,
      customerId,
      transaction: t,
    });
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

// Fetch the active/linked budget for a given trackable (prefer isActive, else most recently updated)
async function budgetsGetActiveByTrackable({
  trackableId,
  customerId,
  ...options
}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.Budget.findOne({
      where: { trackableId, deletedAt: null },
      // Prefer explicitly active; otherwise fall back to most recently updated
      order: [
        ["isActive", "DESC"],
        ["updatedAt", "DESC"],
      ],
      ...options,
      transaction: t,
    });
    await t.commit();
    return row ? row.get({ plain: true }) : null;
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
    const current = await _getBudgetByIdTx({ id, customerId, transaction: t });
    if (!current) throw { status: 404, message: "Budget not found" };
    const isCurrentFinal = String(current.status).toLowerCase() === "final";
    const isMarkingFinal =
      String(data?.status).toLowerCase() === "final" && !isCurrentFinal;
    if (isCurrentFinal && !isMarkingFinal) {
      throw {
        status: 409,
        message: "Final budgets cannot be edited. Create a revision.",
      };
    }
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
    const current = await _getBudgetByIdTx({ id, customerId, transaction: t });
    if (!current) throw { status: 404, message: "Budget not found" };
    const isCurrentFinal = String(current.status).toLowerCase() === "final";
    const isMarkingFinal =
      String(data?.status).toLowerCase() === "final" && !isCurrentFinal;
    if (isCurrentFinal && !isMarkingFinal) {
      throw {
        status: 409,
        message: "Final budgets cannot be edited. Create a revision.",
      };
    }
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

// Convenience helper to link a budget to an trackable
async function budgetsLinkToTrackable({
  id,
  trackableId,
  customerId,
  userId,
  transaction,
  ...options
}) {
  // Thin wrapper over budgetsPatch to make intent explicit
  return budgetsPatch({
    id,
    data: { trackableId, customerId, updatedBy: userId },
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
  linkToTrackable: budgetsLinkToTrackable,
  getActiveByTrackable: budgetsGetActiveByTrackable,
};

module.exports = {
  budgetItems,
  budgets,
  budgetSections,
};
