// utils/db/setClientIdRLS.js
const { sequelize } = require("../db/database"); // adjust path if necessary

async function beginTransactionWithClientContext(clientId) {
  if (!clientId) {
    throw new Error("Client ID is required to set RLS context.");
  }

  if (!/^[a-zA-Z0-9_-]{10}$/.test(clientId)) {
    throw new Error(
      "Invalid clientId format. Must be exactly 10 alphanumeric or -_ characters."
    );
  }

  const t = await sequelize.transaction();

  try {
    await sequelize.query(`SET LOCAL app.current_client_id = '${clientId}'`, {
      transaction: t,
    });
  } catch (error) {
    await t.rollback();
    throw new Error(`SET LOCAL failed, transaction aborted: ${error.message}`);
  }

  return t;
}

module.exports = { beginTransactionWithClientContext };
