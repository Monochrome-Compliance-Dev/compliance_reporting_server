const { Op } = require("sequelize");
const db = require("../db/database");

const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
  getEntitlements,
  getCustomersByAccess,
};

async function getAll() {
  const rows = await db.Customer.findAll();
  return rows.map((r) =>
    typeof r.get === "function" ? r.get({ plain: true }) : r
  );
}

async function getById({ id }) {
  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) throw { status: 404, message: "Customer not found" };
  return typeof customer.get === "function"
    ? customer.get({ plain: true })
    : customer;
}

async function create({ data }) {
  if (!data) throw { status: 400, message: "Missing payload" };
  if (await db.Customer.findOne({ where: { abn: data.abn } })) {
    throw { status: 500, message: "Customer with this ABN already exists" };
  }
  const created = await db.Customer.create({ ...data });
  return created.get({ plain: true });
}

async function update({ id, data }) {
  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) throw { status: 404, message: "Customer not found" };
  Object.assign(customer, data);
  await customer.save();
  return customer.get({ plain: true });
}

async function _delete({ id }) {
  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) throw { status: 404, message: "Customer not found" };
  await customer.destroy();
  return { success: true };
}

async function getEntitlements({ customerId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const entitlements = await db.FeatureEntitlement.findAll({
      where: { customerId },
      attributes: ["feature", "status", "source", "validFrom", "validTo"],
      transaction: t,
    });

    await t.commit();

    if (!entitlements || entitlements.length === 0) {
      return [];
    }
    return entitlements.map((e) => e.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getCustomersByAccess(userId) {
  // Fetch access rows for this user
  const accessRows = await db.CustomerAccess.findAll({
    where: { userId },
  });

  if (!accessRows || accessRows.length === 0) {
    return [];
  }

  // Collect customerIds to look up their business names
  const customerIds = Array.from(
    new Set(
      accessRows
        .map((r) =>
          typeof r.get === "function"
            ? r.get({ plain: true }).customerId
            : r.customerId
        )
        .filter(Boolean)
    )
  );

  if (customerIds.length === 0) {
    return [];
  }

  const customerRows = await db.Customer.findAll({
    where: { id: { [Op.in]: customerIds } },
    attributes: ["id", "businessName"],
  });

  // Normalize to [{ id, businessName }]
  return customerRows.map((row) => {
    const plain =
      typeof row.get === "function" ? row.get({ plain: true }) : row;
    return { id: plain.id, businessName: plain.businessName };
  });
}
