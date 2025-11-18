const db = require("@/db/database");

/**
 * List all customers for Boss admin.
 * This is not tenant-scoped; Boss can see all customers.
 */
async function listCustomers() {
  const customers = await db.Customer.findAll({
    order: [["businessName", "ASC"]],
  });
  return customers.map((c) => c.get({ plain: true }));
}

/**
 * Create a new customer.
 * Expects `data` to contain the customer fields and `userId` for audit.
 */
async function createCustomer({ data, userId }) {
  if (!data) {
    throw { status: 400, message: "Missing payload" };
  }

  if (!userId) {
    // Let this fail loudly at the DB layer if createdBy is NOT NULL,
    // rather than silently creating unaudited records.
    console.warn("[v2/customers.service] createCustomer called without userId");
  }

  if (data.abn) {
    const existing = await db.Customer.findOne({
      where: { abn: data.abn },
    });
    if (existing) {
      throw {
        status: 400,
        message: "A customer with this ABN already exists",
      };
    }
  }

  const created = await db.Customer.create({
    ...data,
    createdBy: userId,
    updatedBy: userId,
  });

  return created.get({ plain: true });
}

/**
 * Update an existing customer by id.
 * Expects `data` to contain the updatable fields and `userId` for audit.
 */
async function updateCustomer({ id, data, userId }) {
  if (!id) {
    throw { status: 400, message: "Missing customer id" };
  }

  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) {
    throw { status: 404, message: "Customer not found" };
  }

  if (!userId) {
    console.warn("[v2/customers.service] updateCustomer called without userId");
  }

  // Shallow assign allowed fields; rely on validation at the model/DB level.
  Object.assign(customer, data, { updatedBy: userId });

  await customer.save();

  return customer.get({ plain: true });
}

/**
 * Soft-delete (paranoid) an existing customer by id.
 */
async function deleteCustomer({ id }) {
  if (!id) {
    throw { status: 400, message: "Missing customer id" };
  }

  const customer = await db.Customer.findOne({ where: { id } });
  if (!customer) {
    throw { status: 404, message: "Customer not found" };
  }

  await customer.destroy(); // paranoid: true will soft-delete

  return { message: "Customer deleted" };
}

module.exports = {
  listCustomers,
  createCustomer,
  updateCustomer,
  deleteCustomer,
};
