// utils/db/setCustomerIdRLS.js
const { sequelize } = require("../db/database"); // adjust path if necessary

async function beginTransactionWithCustomerContext(customerId) {
  if (!customerId) {
    throw new Error("Customer ID is required to set RLS context.");
  }

  if (!/^[a-zA-Z0-9_-]{10}$/.test(customerId)) {
    throw new Error(
      "Invalid customerId format. Must be exactly 10 alphanumeric or -_ characters."
    );
  }

  const t = await sequelize.transaction();

  try {
    await sequelize.query(
      `SET LOCAL app.current_customer_id = '${customerId}'`,
      {
        transaction: t,
      }
    );
  } catch (error) {
    await t.rollback();
    throw new Error(`SET LOCAL failed, transaction aborted: ${error.message}`);
  }

  return t;
}

module.exports = { beginTransactionWithCustomerContext };
