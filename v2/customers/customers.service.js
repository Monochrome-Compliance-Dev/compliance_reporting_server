const { Op } = require("sequelize");
const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

/**
 * v2 Customers Service
 * Mirrors legacy customer.service.js behaviour,
 * but aligned to New World patterns and explicit status codes.
 */

async function listCustomers() {
  const rows = await db.Customer.findAll({
    order: [["businessName", "ASC"]],
  });

  return rows.map((r) =>
    typeof r.get === "function" ? r.get({ plain: true }) : r,
  );
}

async function getCustomerById({ id }) {
  if (!id) {
    throw { status: 400, message: "Missing customer id" };
  }

  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) {
    throw { status: 404, message: "Customer not found" };
  }

  return typeof customer.get === "function"
    ? customer.get({ plain: true })
    : customer;
}

async function createCustomer({ data, userId }) {
  if (!data) {
    throw { status: 400, message: "Missing payload" };
  }

  if (!data.abn) {
    throw { status: 400, message: "ABN is required" };
  }

  const existing = await db.Customer.findOne({
    where: { abn: data.abn },
  });

  if (existing) {
    throw {
      status: 400,
      message: "Customer with this ABN already exists",
    };
  }

  const created = await db.Customer.create({
    ...data,
    createdBy: userId,
    updatedBy: userId,
  });

  return created.get({ plain: true });
}

async function updateCustomer({ id, data, userId }) {
  if (!id) {
    throw { status: 400, message: "Missing customer id" };
  }

  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) {
    throw { status: 404, message: "Customer not found" };
  }

  Object.assign(customer, data, { updatedBy: userId });

  await customer.save();

  return customer.get({ plain: true });
}

async function deleteCustomer({ id }) {
  if (!id) {
    throw { status: 400, message: "Missing customer id" };
  }

  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) {
    throw { status: 404, message: "Customer not found" };
  }

  await customer.destroy(); // paranoid soft-delete if enabled

  return { message: "Customer deleted" };
}

/**
 * Feature entitlements for a specific customer (RLS scoped)
 */
async function getCustomerEntitlements({ customerId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const entitlements = await db.FeatureEntitlement.findAll({
      where: { customerId },
      attributes: ["feature", "status", "source", "validFrom", "validTo"],
      transaction: t,
    });

    await t.commit();

    return (entitlements || []).map((e) =>
      typeof e.get === "function" ? e.get({ plain: true }) : e,
    );
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw {
      status: error.status || 500,
      message: error.message || error,
    };
  }
}

/**
 * Customers a user has access to (used for "act on behalf of")
 */
async function getCustomersByAccess({ userId }) {
  if (!userId) {
    throw { status: 400, message: "Missing userId" };
  }

  const accessRows = await db.CustomerAccess.findAll({
    where: { userId },
  });

  if (!accessRows || accessRows.length === 0) {
    return [];
  }

  const customerIds = Array.from(
    new Set(
      accessRows
        .map((r) =>
          typeof r.get === "function"
            ? r.get({ plain: true }).customerId
            : r.customerId,
        )
        .filter(Boolean),
    ),
  );

  if (customerIds.length === 0) {
    return [];
  }

  const customerRows = await db.Customer.findAll({
    where: { id: { [Op.in]: customerIds } },
    attributes: ["id", "businessName"],
  });

  return customerRows.map((row) => {
    const plain =
      typeof row.get === "function" ? row.get({ plain: true }) : row;
    return { id: plain.id, businessName: plain.businessName };
  });
}

module.exports = {
  listCustomers,
  getCustomerById,
  createCustomer,
  updateCustomer,
  deleteCustomer,
  getCustomerEntitlements,
  getCustomersByAccess,
};
