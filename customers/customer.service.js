const db = require("../db/database");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Customer.findAll();
}

async function getById(customerId, id) {
  const customer = await db.Customer.findOne({ where: { id, customerId } });
  if (!customer) throw { status: 404, message: "Customer not found" };
  return customer;
}

async function create(customerId, params) {
  if (await db.Customer.findOne({ where: { abn: params.abn, customerId } })) {
    throw { status: 500, message: "Customer with this ABN already exists" };
  }
  return await db.Customer.create({ ...params, customerId });
}

async function update(customerId, id, params) {
  const customer = await db.Customer.findOne({ where: { id, customerId } });
  if (!customer) throw { status: 404, message: "Customer not found" };
  Object.assign(customer, params);
  await customer.save();
}

async function _delete(customerId, id) {
  const customer = await db.Customer.findOne({ where: { id, customerId } });
  if (!customer) throw { status: 404, message: "Customer not found" };
  await customer.destroy();
}
