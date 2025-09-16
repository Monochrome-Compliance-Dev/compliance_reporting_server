/**
 * PTRS Reference Data Service (step 1)
 * Adds employees.list and employees.create using the same transaction/RLS pattern as ptrs.service.js.
 */

const db = require("../db/database");
const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");

// --- helpers ----------------------------------------------------------------
function plain(row) {
  return row?.get ? row.get({ plain: true }) : row;
}

async function commitOrRollback(t, ok) {
  try {
    if (ok) {
      if (!t.finished) await t.commit();
    } else {
      if (!t.finished) await t.rollback();
    }
  } catch {
    // swallow
  }
}

// --- employees: list, create ------------------------------------------------
const employees = {
  /**
   * List employees for a customer.
   * @param {Object} params
   * @param {string} params.customerId
   * @param {Object} [params.where]
   * @param {Array}  [params.order=[["createdAt","DESC"]]]
   */
  async list({
    customerId,
    where = {},
    order = [["createdAt", "DESC"]],
    ...options
  } = {}) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const rows = await db.EmployeeRef.findAll({
        where: { customerId, ...where },
        order,
        ...options,
        transaction: t,
      });
      ok = true;
      return rows.map(plain);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  /**
   * Create an employee for a customer.
   * @param {Object} params
   * @param {Object} params.data
   * @param {string} params.customerId
   * @param {string} [params.userId]
   */
  async create({ data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const row = await db.EmployeeRef.create(
        {
          ...data,
          customerId,
          createdBy: data.createdBy || userId,
          updatedBy: data.updatedBy || userId,
        },
        { ...options, transaction: t }
      );
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  /**
   * Update an employee for a customer (full update).
   * @param {Object} params
   * @param {string|number} params.id
   * @param {Object} params.data
   * @param {string} params.customerId
   * @param {string} params.userId
   */
  async update({ id, data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      await db.EmployeeRef.update(
        { ...data, updatedBy: userId },
        { where: { id, customerId }, ...options, transaction: t }
      );
      const row = await db.EmployeeRef.findOne({
        where: { id, customerId },
        transaction: t,
      });
      if (!row) {
        throw {
          status: 404,
          message: "Employee not found (or blocked by RLS).",
        };
      }
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  /**
   * Patch an employee for a customer (partial update).
   * @param {Object} params
   * @param {string|number} params.id
   * @param {Object} params.data
   * @param {string} params.customerId
   * @param {string} params.userId
   */
  async patch({ id, data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const [count] = await db.EmployeeRef.update(
        { ...data, updatedBy: userId },
        { where: { id, customerId }, ...options, transaction: t }
      );
      if (count === 0) {
        throw {
          status: 404,
          message: "Employee not found (or blocked by RLS).",
        };
      }
      const row = await db.EmployeeRef.findOne({
        where: { id, customerId },
        transaction: t,
      });
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  /**
   * Delete an employee (paranoid by default).
   * @param {Object} params
   * @param {string|number} params.id
   * @param {string} params.customerId
   * @param {boolean} [params.hard=false]
   */
  async delete({ id, customerId, hard = false, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const count = await db.EmployeeRef.destroy({
        where: { id, customerId },
        force: !!hard,
        ...options,
        transaction: t,
      });
      if (count === 0) {
        throw {
          status: 404,
          message: "Employee not found (or blocked by RLS).",
        };
      }
      ok = true;
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },
};

const intraCompanies = {
  /**
   * List intra-company references for a customer.
   */
  async list({
    customerId,
    where = {},
    order = [["createdAt", "DESC"]],
    ...options
  } = {}) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const rows = await db.IntraCompanyRef.findAll({
        where: { customerId, ...where },
        order,
        ...options,
        transaction: t,
      });
      ok = true;
      return rows.map(plain);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async create({ data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const row = await db.IntraCompanyRef.create(
        {
          ...data,
          customerId,
          createdBy: data.createdBy || userId,
          updatedBy: data.updatedBy || userId,
        },
        { ...options, transaction: t }
      );
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async update({ id, data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      await db.IntraCompanyRef.update(
        { ...data, updatedBy: userId },
        { where: { id, customerId }, ...options, transaction: t }
      );
      const row = await db.IntraCompanyRef.findOne({
        where: { id, customerId },
        transaction: t,
      });
      if (!row) {
        throw {
          status: 404,
          message: "IntraCompany not found (or blocked by RLS).",
        };
      }
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async patch({ id, data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const [count] = await db.IntraCompanyRef.update(
        { ...data, updatedBy: userId },
        { where: { id, customerId }, ...options, transaction: t }
      );
      if (count === 0) {
        throw {
          status: 404,
          message: "IntraCompany not found (or blocked by RLS).",
        };
      }
      const row = await db.IntraCompanyRef.findOne({
        where: { id, customerId },
        transaction: t,
      });
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async delete({ id, customerId, hard = false, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const count = await db.IntraCompanyRef.destroy({
        where: { id, customerId },
        force: !!hard,
        ...options,
        transaction: t,
      });
      if (count === 0) {
        throw {
          status: 404,
          message: "IntraCompany not found (or blocked by RLS).",
        };
      }
      ok = true;
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },
};

const keywords = {
  /**
   * List keywords for a customer.
   * @param {Object} params
   * @param {string} params.customerId
   * @param {Object} [params.where]
   * @param {Array}  [params.order=[["createdAt","DESC"]]]
   */
  async list({
    customerId,
    where = {},
    order = [["createdAt", "DESC"]],
    ...options
  } = {}) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const rows = await db.ExclusionKeywordCustomerRef.findAll({
        where: { customerId, ...where },
        order,
        ...options,
        transaction: t,
      });
      ok = true;
      return rows.map(plain);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  /**
   * Create a keyword for a customer.
   * @param {Object} params
   * @param {Object} params.data
   * @param {string} params.customerId
   * @param {string} [params.userId]
   */
  async create({ data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const row = await db.ExclusionKeywordCustomerRef.create(
        {
          ...data,
          customerId,
          createdBy: data.createdBy || userId,
          updatedBy: data.updatedBy || userId,
        },
        { ...options, transaction: t }
      );
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async update({ id, data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      await db.ExclusionKeywordCustomerRef.update(
        { ...data, updatedBy: userId },
        { where: { id, customerId }, ...options, transaction: t }
      );
      const row = await db.ExclusionKeywordCustomerRef.findOne({
        where: { id, customerId },
        transaction: t,
      });
      if (!row) {
        throw {
          status: 404,
          message: "Keyword not found (or blocked by RLS).",
        };
      }
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async patch({ id, data, customerId, userId, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const [count] = await db.ExclusionKeywordCustomerRef.update(
        { ...data, updatedBy: userId },
        { where: { id, customerId }, ...options, transaction: t }
      );
      if (count === 0) {
        throw {
          status: 404,
          message: "Keyword not found (or blocked by RLS).",
        };
      }
      const row = await db.ExclusionKeywordCustomerRef.findOne({
        where: { id, customerId },
        transaction: t,
      });
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async delete({ id, customerId, hard = false, ...options }) {
    const t = await beginTransactionWithCustomerContext(customerId);
    let ok = false;
    try {
      const count = await db.ExclusionKeywordCustomerRef.destroy({
        where: { id, customerId },
        force: !!hard,
        ...options,
        transaction: t,
      });
      if (count === 0) {
        throw {
          status: 404,
          message: "Keyword not found (or blocked by RLS).",
        };
      }
      ok = true;
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },
};

const govEntities = {
  /**
   * List government entities (GLOBAL, no customerId).
   */
  async list({ where = {}, order = [["createdAt", "DESC"]], ...options } = {}) {
    const t = await db.sequelize.transaction();
    let ok = false;
    try {
      const rows = await db.GovEntityRef.findAll({
        where,
        order,
        ...options,
        transaction: t,
      });
      ok = true;
      return rows.map(plain);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async create({ data, userId, ...options }) {
    const t = await db.sequelize.transaction();
    let ok = false;
    try {
      const row = await db.GovEntityRef.create(
        {
          ...data,
          createdBy: data.createdBy || userId,
          updatedBy: data.updatedBy || userId,
        },
        { ...options, transaction: t }
      );
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async update({ id, data, userId, ...options }) {
    const t = await db.sequelize.transaction();
    let ok = false;
    try {
      await db.GovEntityRef.update(
        { ...data, updatedBy: userId },
        { where: { id }, ...options, transaction: t }
      );
      const row = await db.GovEntityRef.findOne({
        where: { id },
        transaction: t,
      });
      if (!row) {
        throw { status: 404, message: "GovEntity not found." };
      }
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async patch({ id, data, userId, ...options }) {
    const t = await db.sequelize.transaction();
    let ok = false;
    try {
      const [count] = await db.GovEntityRef.update(
        { ...data, updatedBy: userId },
        { where: { id }, ...options, transaction: t }
      );
      if (count === 0) {
        throw { status: 404, message: "GovEntity not found." };
      }
      const row = await db.GovEntityRef.findOne({
        where: { id },
        transaction: t,
      });
      ok = true;
      return plain(row);
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },

  async delete({ id, hard = false, ...options }) {
    const t = await db.sequelize.transaction();
    let ok = false;
    try {
      const count = await db.GovEntityRef.destroy({
        where: { id },
        force: !!hard,
        ...options,
        transaction: t,
      });
      if (count === 0) {
        throw { status: 404, message: "GovEntity not found." };
      }
      ok = true;
    } catch (error) {
      throw { status: error.status || 500, message: error.message || error };
    } finally {
      await commitOrRollback(t, ok);
    }
  },
};

// at bottom of ptrs/reference-data.service.js
module.exports = {
  employees,
  employee: employees, // alias
  intraCompanies,
  intraCompany: intraCompanies, // alias
  keywords,
  govEntities,
  gov: govEntities, // alias
};
