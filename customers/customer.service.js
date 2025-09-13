const db = require("../db/database");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
  getEntitlements,
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
  const entitlements = await db.FeatureEntitlement.findAll({
    where: { customerId },
    attributes: ["feature", "status", "source", "validFrom", "validTo"],
  });
  if (!entitlements || entitlements.length === 0) {
    return [];
  }
  return entitlements.map((e) =>
    typeof e.get === "function" ? e.get({ plain: true }) : e
  );
}
