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
  getUtilisation,
};

async function getAll({ customerId, ...options } = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.Resource.findAll({
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

async function getById({ id, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.Resource.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!row) throw { status: 404, message: "Resource not found" };
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
    const result = await db.Resource.create(
      {
        ...data,
        customerId,
        createdBy: data.createdBy,
        updatedBy: data.updatedBy ?? data.createdBy,
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
    await db.Resource.update(
      { ...data, updatedBy: userId ?? data.updatedBy },
      { where: { id }, ...options, transaction: t }
    );
    const result = await db.Resource.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Resource not found" };
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
    const [count, [updated]] = await db.Resource.update(
      { ...data, updatedBy: userId ?? data.updatedBy },
      { where: { id }, returning: true, ...options, transaction: t }
    );
    if (count === 0)
      throw {
        status: 404,
        message: "Resource not found or update blocked by RLS.",
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
    const count = await db.Resource.destroy({
      where: { id },
      ...options,
      transaction: t,
    });
    if (count === 0)
      throw {
        status: 404,
        message: "Resource not found or delete blocked by RLS.",
      };
    await t.commit();
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getUtilisation({ customerId, from, to, includeNonBillable }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // Example: aggregate total hours or contributions per resource
    const rows = await db.Resource.findAll({
      attributes: [
        "id",
        "name",
        "position",
        [
          db.sequelize.fn("COUNT", db.sequelize.col("Contributions.id")),
          "entries",
        ],
        [
          db.sequelize.fn("SUM", db.sequelize.col("Contributions.hours")),
          "totalHours",
        ],
      ],
      include: [
        {
          model: db.Contribution,
          attributes: [],
          where: {
            ...(from && {
              createdAt: { [db.Sequelize.Op.gte]: new Date(from) },
            }),
            ...(to && { createdAt: { [db.Sequelize.Op.lte]: new Date(to) } }),
            ...(includeNonBillable ? {} : { isBillable: true }),
          },
          required: false,
        },
      ],
      group: ["Resource.id"],
      order: [["name", "ASC"]],
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
