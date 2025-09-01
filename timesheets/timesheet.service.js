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
  // rows
  listByTimesheet,
  createRow,
  updateRow,
  patchRow,
  deleteRow,
  utilisation,
};

async function getAll({ customerId, ...options } = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.Timesheet.findAll({
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
    const row = await db.Timesheet.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!row) throw { status: 404, message: "Timesheet not found" };
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
    const result = await db.Timesheet.create(
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
    await db.Timesheet.update(
      { ...data, updatedBy: userId },
      { where: { id }, ...options, transaction: t }
    );
    const result = await db.Timesheet.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Timesheet not found" };
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
    const [count, [updated]] = await db.Timesheet.update(
      { ...data, updatedBy: userId },
      { where: { id }, returning: true, ...options, transaction: t }
    );
    if (count === 0)
      throw {
        status: 404,
        message: "Timesheet not found or update blocked by RLS.",
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
    const count = await db.Timesheet.destroy({
      where: { id },
      ...options,
      transaction: t,
    });
    if (count === 0)
      throw {
        status: 404,
        message: "Timesheet not found or delete blocked by RLS.",
      };
    await t.commit();
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

// ---- Rows ----
async function listByTimesheet({ timesheetId, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rows = await db.TimesheetRow.findAll({
      where: { timesheetId },
      order: [["date", "ASC"]],
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

async function createRow({ timesheetId, data, customerId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const result = await db.TimesheetRow.create(
      {
        ...data,
        timesheetId,
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

async function updateRow({ id, data, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.TimesheetRow.update(
      { ...data, updatedBy: userId },
      { where: { id }, ...options, transaction: t }
    );
    const result = await db.TimesheetRow.findOne({
      where: { id },
      ...options,
      transaction: t,
    });
    if (!result) throw { status: 404, message: "Timesheet row not found" };
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function patchRow({
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
    const [count, [updated]] = await db.TimesheetRow.update(
      { ...data, updatedBy: userId },
      { where: { id }, returning: true, ...options, transaction: t }
    );
    if (count === 0)
      throw {
        status: 404,
        message: "Timesheet row not found or update blocked by RLS.",
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

async function deleteRow({ id, customerId, userId, ...options }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const count = await db.TimesheetRow.destroy({
      where: { id },
      ...options,
      transaction: t,
    });
    if (count === 0)
      throw {
        status: 404,
        message: "Timesheet row not found or delete blocked by RLS.",
      };
    await t.commit();
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

// ---- Aggregation: Utilisation ----
async function utilisation({ customerId, from, to, includeNonBillable } = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  const { Op, fn, col } = db.Sequelize;
  try {
    // Build date span
    let start = null;
    let end = null;
    if (from) start = new Date(from);
    if (to) {
      const tmp = new Date(to);
      // inclusive to end of day
      tmp.setHours(23, 59, 59, 999);
      end = tmp;
    }

    // Base where for TimesheetRow
    const where = {};
    if (start && end) where.date = { [Op.between]: [start, end] };
    else if (start) where.date = { [Op.gte]: start };
    else if (end) where.date = { [Op.lte]: end };

    // Only apply billable filter if the column exists
    const hasBillable = !!(
      db.TimesheetRow?.rawAttributes && db.TimesheetRow.rawAttributes.isBillable
    );
    if (hasBillable && includeNonBillable !== true) {
      where.isBillable = true;
    }

    // Group hours by timesheet (header carries resourceId) and engagement
    const groupedByTs = await db.TimesheetRow.findAll({
      attributes: [
        "timesheetId",
        "engagementId",
        [fn("sum", col("hours")), "hours"],
      ],
      where,
      group: ["timesheetId", "engagementId"],
      transaction: t,
      raw: true,
    });

    // Load header records to resolve resourceId per timesheetId
    const timesheetIds = Array.from(
      new Set(
        groupedByTs.map((g) => String(g.timesheetId || "")).filter(Boolean)
      )
    );
    let tsById = new Map();
    if (timesheetIds.length > 0) {
      const headers = await db.Timesheet.findAll({
        attributes: ["id", "resourceId"],
        where: { id: { [Op.in]: timesheetIds } },
        transaction: t,
        raw: true,
      });
      tsById = new Map(headers.map((h) => [String(h.id), h]));
    }

    // Index results by resource
    const byResource = new Map();
    for (const row of groupedByTs) {
      const tsId = String(row.timesheetId || "");
      const header = tsById.get(tsId);
      const rid = header && header.resourceId ? String(header.resourceId) : "";
      if (!rid) continue;
      const list = byResource.get(rid) || [];
      list.push({
        engagementId: row.engagementId ? String(row.engagementId) : "",
        hours: Number(row.hours) || 0,
      });
      byResource.set(rid, list);
    }

    const resourceIds = Array.from(byResource.keys());
    if (resourceIds.length === 0) {
      await t.commit();
      return [];
    }

    // Load resources (name/role/capacity)
    const resources = await db.Resource.findAll({
      where: { id: { [Op.in]: resourceIds } },
      transaction: t,
      raw: true,
    });
    const resById = new Map(resources.map((r) => [String(r.id), r]));

    // Load engagements for name lookup
    const engagementIds = Array.from(
      new Set(
        groupedByTs.map((g) => String(g.engagementId || "")).filter(Boolean)
      )
    );
    let engById = new Map();
    if (engagementIds.length > 0 && db.Engagement) {
      const engagements = await db.Engagement.findAll({
        where: { id: { [Op.in]: engagementIds } },
        transaction: t,
        raw: true,
      });
      engById = new Map(engagements.map((e) => [String(e.id), e]));
    }

    // Compute span days for capacity proration
    const now = new Date();
    const spanStart =
      start ||
      new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - (now.getDay() === 0 ? 6 : now.getDay() - 1)
      );
    const spanEnd =
      end ||
      new Date(
        spanStart.getFullYear(),
        spanStart.getMonth(),
        spanStart.getDate() + 6,
        23,
        59,
        59,
        999
      );
    const daysInSpan = Math.max(
      1,
      Math.ceil((spanEnd - spanStart) / (1000 * 60 * 60 * 24)) + 0
    ); // inclusive-ish
    const spanWeeks = daysInSpan / 7.0;

    // Shape the payload
    const result = [];
    for (const rid of resourceIds) {
      const res = resById.get(rid) || {};
      const capWeekly = Number(res.capacityHoursPerWeek) || 40;
      const capacityHours = Number((capWeekly * spanWeeks).toFixed(2));
      const items = byResource.get(rid) || [];

      // Attach engagement names
      const byEngagement = items.map((it) => ({
        engagementId: it.engagementId,
        engagementName: (engById.get(it.engagementId) || {}).name || "",
        hours: Number(it.hours) || 0,
      }));
      const loggedHours = byEngagement.reduce(
        (s, it) => s + (Number(it.hours) || 0),
        0
      );
      const utilPct =
        capacityHours > 0
          ? Number(((loggedHours / capacityHours) * 100).toFixed(1))
          : 0;

      result.push({
        resourceId: rid,
        resourceName: res.name || "",
        role: res.role || "",
        capacityHours,
        loggedHours: Number(loggedHours.toFixed(1)),
        utilPct,
        byEngagement,
      });
    }

    await t.commit();
    return result;
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}
